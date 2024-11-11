/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This read-only channel wrapper implements caching in terms of pages (fixed size unit). 
 * Any cache miss loads the entire page from the delegate channel, which can be costly.
 * The page eviction strategy is to drop the least recently used (LRU).
 * <p>
 * To minimize costs, pages should be smaller and more numerous, within reasons, as there is overhead for organizing the pages.
 * This implementation limits the page size from 16 bytes to 2 MB, but a more decent range would be 1kB to 32kB depending on applications.
 * <p>
 * Any live changes to the delegate channel content is unsupported as there is no staleness detection.
 */
public class CachingSeekableReadByteChannel extends FileChannel {
	
	private int pageSize = 8<<10; //8kiB
	
	private Map<Long,ByteBuffer> cache;
	private long pos;
	private long sourceTotalSize = -1;
	private SeekableByteChannel source;
	
	/**
	 * @param pageSize the size in bytes of a page to be cached
	 * @param maxPages the number of pages to cache.
	 * @param source the source being cached.
	 */
	@SuppressWarnings("serial")
	public CachingSeekableReadByteChannel(int pageSize, int maxPages, SeekableByteChannel source) throws IOException {
		if(pageSize<16 || pageSize > 1<<21)
			throw new IllegalArgumentException("page size not in range 2^4 to 2^21");
		Objects.requireNonNull(source);
		
		this.sourceTotalSize = source.size();
		if(sourceTotalSize<0)
			throw new IOException("source size is not available");
		this.pageSize = pageSize;
		this.source = source;
		
		cache = new LinkedHashMap<>(16, 0.5f, true) {
			protected boolean removeEldestEntry(Map.Entry<Long,ByteBuffer> eldest) {
				return size()>maxPages;
			}
		};
	}
	
	/**
	 * @return true if the position is in a page which is currently in the cache.
	 */
	public boolean hasPage(long position) {
		long pageId = position/pageSize;
		ByteBuffer pageBuffer = cache.get(pageId);
		return pageBuffer!=null;
	}
	
	private ByteBuffer fetchPage(long position) throws IOException {
		long pageId = position/pageSize;
		ByteBuffer pageBuffer = cache.get(pageId);
		if(pageBuffer!=null)
			return pageBuffer;
		
		long startPos = pageId*pageSize; //beginning of page
		long endPos = Math.min(startPos+pageSize, sourceTotalSize);
		final int qty = (int)(endPos - startPos);
		pageBuffer = ByteBuffer.allocateDirect(qty);
		
		source.position(startPos);
		
		while(pageBuffer.hasRemaining()) {
			long n = source.read(pageBuffer);
			if(n<0) {
				//I though about caching the partial page anyway, because otherwise it will try downloading over and over...
				//but the fact is, either the source reported a wrong size, or did not provide enough bytes.
				//Either way, the buffer is incomplete and cannot be relied on.
				throw new EOFException("partial page found which is not at end of file;"
					+ " expected page 0x"+Long.toHexString(pageId)
					+ " [0x"+Long.toHexString(startPos)+" - 0x"+Long.toHexString(endPos-1)+"]"
					+ ", "+qty+" bytes"
					+ "; missing "+pageBuffer.remaining()+" bytes."
				);
			}
		}
		
		pageBuffer.flip();
		cache.put(pageId, pageBuffer);
		
		return pageBuffer;
	}
	
	//---------
	
	
	
	@Override
	public int read(ByteBuffer dst) throws IOException {
		int qty = read(dst, pos);
		pos += qty;
		return qty;
	}

	@Override
	public int read(ByteBuffer dst, long position) throws IOException {
		int qty = dst.remaining();
		if(qty<=0)
			return 0;
		if(position>=sourceTotalSize)
			return -1; //EOF
		
		ByteBuffer pageBuffer = fetchPage(position);
		if(pageBuffer==null)
			throw new IOException("unable to fetch the page under position 0x"+Long.toHexString(pos));
		
		int bufstart = (int)(position % pageSize);
		qty = Math.min(qty, pageBuffer.limit()-bufstart);
		
		dst.put(pageBuffer.slice(bufstart, qty));
		return qty;
	}
	
	@Override
	public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
		if(pos>=sourceTotalSize)
			return -1; //EOF
		
		ByteBuffer pageBuffer = fetchPage(pos);
		if(pageBuffer==null)
			throw new IOException("unable to fetch the page under position 0x"+Long.toHexString(pos));
		
		int bufstart = (int)(pos % pageSize);
		int avail = pageBuffer.limit()-bufstart;
		int total = 0;
		
		for(int i=offset, n=offset+length; i<n; i++) {
			int qty = dsts[i].remaining();
			if(qty<=0)
				continue;
			
			qty = Math.min(qty, avail);

			dsts[i].put(pageBuffer.slice(bufstart, qty));
			total += qty;
			pos += qty;
			bufstart += qty;
			avail -= qty;
		}
		
		return total;
	}

	@Override
	public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
		if(count<0)
			return 0;
		
		long written = 0;
		long currentPos = position;
		
		while(written<count && currentPos<sourceTotalSize) {
			ByteBuffer pageBuffer = fetchPage(currentPos);
			if(pageBuffer==null)
				throw new IOException("unable to fetch the page under position 0x"+Long.toHexString(pos));
			
			int bufstart = (int)(currentPos % pageSize);
			int qty = (int)Math.min(pageBuffer.limit()-bufstart, count-written);
			ByteBuffer sub = pageBuffer.slice(bufstart, qty);
			
			while(sub.hasRemaining())
				written += target.write(sub);
			
			currentPos += qty;
		}
		
		return written;
	}

	
	
	@Override
	public long position() throws IOException {
		return pos;
	}

	@Override
	public FileChannel position(long newPosition) throws IOException {
		this.pos = newPosition;
		return this;
	}

	@Override
	public long size() throws IOException {
		return sourceTotalSize;
	}
	
	
	
	//---- NOT SUPPORTED ---------
	
	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	public int write(ByteBuffer src) throws IOException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	public int write(ByteBuffer src, long position) throws IOException {
		throw new UnsupportedOperationException();
	}


	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
		throw new UnsupportedOperationException();
	}


	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	public FileChannel truncate(long size) throws IOException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	public void force(boolean metaData) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	public FileLock lock(long position, long size, boolean shared) throws IOException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	public FileLock tryLock(long position, long size, boolean shared) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Throws UnsupportedOperationException.
	 */
	@Override
	protected void implCloseChannel() throws IOException {
		//
	}
	
	
}
