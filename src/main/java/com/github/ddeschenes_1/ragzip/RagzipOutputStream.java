/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import static com.github.ddeschenes_1.ragzip.GzipUtils.writeEmptyGzipRAMetadata;

import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.LinkedList;
import java.util.zip.GZIPOutputStream;

import com.github.ddeschenes_1.ragzip.Utils.NoCloseChannel;
import com.github.ddeschenes_1.ragzip.Utils.NoCloseOutputStream;

/**
 * This implementation of a ragzip producer expects a SeekableByteChannel or an OutputStream.
 * The partitioning of the input data will be done in pages of size  = 2^pgSizeExponent
 * and indexed with 2^indexSizeExponent records per index at each levels.
 * <p>
 * Page size exponent must be in [9..30] (512 bytes to 1 GB) 
 * and index size exponent in [1..12] (2 to 4096).
 * <p>
 * This implementation supports all max 53 levels of indexes.
 * There may be no index at all (number of levels = 0) if the contant is less or equals to 1 full page.
 */
public class RagzipOutputStream extends OutputStream {
	
	interface PositionSupplier {
		long getPosition() throws IOException; //can't use LongSupplier because of IOException
	}
	
	/**
	 * The ragzip specification version 1.0 (expressed as 0x0001_0000) implemented by this class (not the version of the code),
	 * as it would be embeded in the ragzip footer.  
	 */
	public static final int VERSION_1_0 = 0x00010000;
	
	/**
	 * The forever-fixed-length of 64 bytes of the ragzip footer.
	 */
	public static final int FOOTER_LENGTH64 = 64;

	/**
	 * The foverer-maximum ragzip format capacity of 2^62 (0x40000000_00000000L) bytes. 
	 */
	public static final long MAX_CAPACITY = 0x40000000_00000000L;
	
	//===========================
	
	LinkedList<Extension> extensions = new LinkedList<>();
	
	final int pageSizeExponent;
	final int pageMaxSize;
	
	final int indexSizeExponent;
	final int indexMaxSize;
	
	final SeekableByteChannel outputChannel;
	final OutputStream outputStream;
	final PositionSupplier positionSupplier; 
	final NoCloseOutputStream ncos;
	
	GZIPOutputStream gzos;
	long totalUncompressed; //total bytes stored
	int pageUncompressedCount; //0..pagesize-1
	
	long nextPageStartOffset;
	int[] levelsOccupancies = new int[54]; //[0] is never used, 1..53
	ByteBuffer[] indexes = new ByteBuffer[54];//[0] is never used, 1..53
	
	boolean closed;

	/**
	 * Starts a new ragzip in the given outputstream.
	 * <p>
	 * This constructor obviously cannot read to resume an existing ragzip file. The destination storage is assumed to begin 
	 * this ragzip stream at offset 0, otherwise the offsets written in the metadata will be wrong
	 * as they are relative to the beginning of the 1st page it has written.
	 * <p>
	 * If the destination stream is not the beginning of the ragzip, then later upon reading the file channel any seek to 
	 * an offset would need to be translated to another position. This would require that the SeekableByteChannel expected by the RagzipFileChannel 
	 * as a source, will be responsible to translate those offsets. We do not recommend that usage as the files will not be standard.
	 * <p>
	 * 
	 * @param outputStream  the destination outputstream to append
	 * @param pgSizeExponent the exponent of 2 to define the page size (ex: 13 for a page size of 2^13 = 8192 bytes).
	 * @param indexSizeExponent the exponent of 2 to define the index number of slots (ex: 12 for 2^12 = 4096 slots).
	 */
	public RagzipOutputStream(OutputStream outputStream, int pgSizeExponent, int indexSizeExponent) throws IOException {
		this(null, outputStream, pgSizeExponent, indexSizeExponent, false);
	}
	
	/**
	 * Start a new ragzip content, attempting to resume the outputChannel if non-empty.
	 * Equivalent to this(outputChannel, pgSizeExponent, indexSizeExponent, false);
	 * 
	 * @see RagzipOutputStream#RagzipOutputStream(SeekableByteChannel, int, int, boolean)
	 */
	public RagzipOutputStream(SeekableByteChannel outputChannel, int pgSizeExponent, int indexSizeExponent) throws IOException {
		this(outputChannel, pgSizeExponent, indexSizeExponent, false);
	}
	
	/**
	 * Start or resume ragzip content.
	 * If the truncate==true, the file is cloberred and a new ragzip is started.
	 * If truncate==false, then the outputChannel is read for the ragzip footer, extensions, tail indexes and page
	 * to resume the ragzip where it left. The ragzip content given is assumed to start at offset 0 for the resumption
	 * to make sense, otherwise the offsets written in the metadata will be wrong.
	 * 
	 * Resumption will fail if the requested page and index size exponents are different than the ones in the file, unless the
	 * the file logical size is 0, in which case the file is truncated to 0 and started as new.
	 * 
	 * The position tracking relies on the outputChannel.position(), not an internal counter of bytes written as in the outputstream mode.
	 *    
	 * @param outputChannel the destination to append or resume
	 * @param pgSizeExponent the exponent of 2 to define the page size (ex: 13 for a page size of 2^13 = 8192 bytes).
	 * @param indexSizeExponent the exponent of 2 to define the index number of slots (ex: 12 for 2^12 = 4096 slots).
	 * @param truncate when false, an attempt to resume an non-empty destination is made; when true, the destination will cloberred. 
	 */
	public RagzipOutputStream(SeekableByteChannel outputChannel, int pgSizeExponent, int indexSizeExponent, boolean truncate) throws IOException {
		this(outputChannel, null, pgSizeExponent, indexSizeExponent, truncate);
	}
	
	
	
	
	private RagzipOutputStream(SeekableByteChannel outputChannel, OutputStream outputStream, int pgSizeExponent, int indexSizeExponent, boolean truncate) throws IOException {
		if(outputChannel==null && outputStream==null)
			throw new IllegalArgumentException("null output");
		if(pgSizeExponent<9 || pgSizeExponent>30)
			throw new IllegalArgumentException("page size exponent must be in range [9..30] (512 bytes to 1 GB)");
		if(indexSizeExponent<1 || indexSizeExponent>12)
			throw new IllegalArgumentException("index size exponent must be in range [1..12] (2 to 4096 records)");
		
		this.pageSizeExponent = pgSizeExponent;
		this.indexSizeExponent = indexSizeExponent;
		this.pageMaxSize = 1 << pgSizeExponent;
		this.indexMaxSize = 1 << indexSizeExponent;
		
		if(outputChannel!=null) {
			this.outputChannel = outputChannel;
			this.outputStream = null;
			if(truncate) {
				outputChannel.truncate(0);
				nextPageStartOffset = 0;
			} else if(outputChannel.size()>0) {
				resumeRagzip(); //will set the outputChannel at the proper position
			}
			ncos = new NoCloseOutputStream(new BufferedOutputStream(Channels.newOutputStream(outputChannel), 8192));
			positionSupplier = outputChannel::position;
			
		} else {
			if(truncate)
				throw new IllegalArgumentException("cannot use truncate==true on outputstream destination");
			this.outputChannel = null;
			this.outputStream = outputStream;
			CountOS countOS = new CountOS(new BufferedOutputStream(outputStream, 8192));
			ncos = new NoCloseOutputStream(countOS);
			positionSupplier = () -> countOS.n;
			nextPageStartOffset = 0; //presummed to be at beginning
		}
		
		gzos = new GZIPOutputStream(ncos, 8192);
	}
	
	void resumeRagzip() throws IOException {
		//try to read old ragzip
		try (RagzipFileChannel rc = new RagzipFileChannel(new NoCloseChannel(outputChannel), 0, 1)) { //at least 1 cached index per level to hold the partial indexes
			if(rc.pageSizeExponent != pageSizeExponent || rc.indexSizeExponent != indexSizeExponent)
				throw new IllegalArgumentException("cannot append ragzip of distinct page size or index size exponents");
			if(rc.version != VERSION_1_0)
				throw new IllegalArgumentException("cannot append ragzip of a distinct version");
			
			this.extensions = rc.extensions;
			
			if(rc.logicalSize<=0) {
				//much simpler to just start over
				outputChannel.truncate(0);
			} else {
				rc.position(rc.logicalSize-1); //warmup the indexes
				for(int level=rc.numberOfLevels; level>0; level--) {
					this.indexes[level] = ByteBuffer.allocate(indexMaxSize<<3);
					ByteBuffer index = rc.indexesCache[level].values().iterator().next(); //exactly one index for sure.
					this.indexes[level].put(index); //fill up the known offsets from tail partial index
					this.levelsOccupancies[level] = index.limit()/8;
				}
				
				//recover where the last page started:
				this.nextPageStartOffset = 0;
				if(rc.numberOfLevels > 1) {
					ByteBuffer partialLevel1Index = this.indexes[1];
					//restore where the last page started
					this.nextPageStartOffset = partialLevel1Index.getLong(8*(this.levelsOccupancies[1]-1));//partialLevel1Index.position()-8);
				}
				
				//recover where the last page ended:
				//this implementation will not reprocess the partial page
				//but we need to know where it ends to position the channel!
				//This will be the current location of the level1 index if nay, or the first extension if any, or the footer.
				final long offsetToReopen;
				if(rc.numberOfLevels>0) {
					if(rc.numberOfLevels==1) {
						offsetToReopen = rc.topIndexOffset;
					} else {
						//we have level2, then the position of last level 1 is in there
						ByteBuffer partialLevel2Index = this.indexes[2];
						offsetToReopen = partialLevel2Index.getLong(8*(this.levelsOccupancies[2]-1));//partialLevel2Index.position()-8);
					}
						
				} else if(!extensions.isEmpty()) {
					//offsetToReopen = extensions.size()==1 ? rc.extensionsOffset : extensions.get(1).previousExtensionOffset;
					offsetToReopen = extensions.getFirst()._selfOffset;//equivalent but more direct
				} else {
					//the laste page ended with the start of the footer.
					offsetToReopen = outputChannel.size() - FOOTER_LENGTH64;
				}
				
				outputChannel.truncate(offsetToReopen); //trim the indexes, extensions, footer.
				outputChannel.position(offsetToReopen);
				
				this.totalUncompressed = rc.logicalSize;
			}
		}
	}
	
	/**
	 * Add a custom extension to the ragzip footer.
	 * 
	 * @see Extension
	 */
	public RagzipOutputStream appendExtension(Extension ext) throws IOException {
		if(ext.payload.length>Extension.MAX_EXT_PAYLOAD)
			throw new IOException("Extension payload exceeds "+Extension.MAX_EXT_PAYLOAD+" ("+ext.payload.length+")");
		
		if(extensions.size()>=Extension.MAX_EXT_COUNT)
			throw new IOException("Too many extensions (cannot exceed "+Extension.MAX_EXT_COUNT+")");
		
		extensions.addLast(ext);
		return this;
	}
	
	private void throwIfRagzipMaxExceeded(int qty) throws IOException {
		if(totalUncompressed+qty >= MAX_CAPACITY)
			throw new IOException("max capacity would be exceeded for this implementation ("+Long.toHexString(MAX_CAPACITY)+")");
	}
	
	@Override
	public void write(int b) throws IOException {
		if(closed)
			throw new IOException("stream is closed");
		throwIfRagzipMaxExceeded(1);
		
		flushPageIfFullAndOpenNew();
		gzos.write(b);
		pageUncompressedCount++;
		totalUncompressed++;
	}
	
	@Override
	public void write(byte[] ba, int off, int len) throws IOException {
		if(closed)
			throw new IOException("stream is closed");
		throwIfRagzipMaxExceeded(len);
		
		final int end = off+len;
		if ((off | len | end | (ba.length - end)) < 0)
			throw new IndexOutOfBoundsException();
		
		while(off<end) {
			flushPageIfFullAndOpenNew();
			
			//fill page
			int pgfree = pageMaxSize-pageUncompressedCount;
			int qty = Math.min(pgfree,len);
			gzos.write(ba, off, qty);
			pageUncompressedCount += qty;
			totalUncompressed += qty;
			off += qty;
			len -= qty;
		}
	}
	
	/**
	 * Flush the internal gzip output stream of the page, and therefore the underlying outputstream.
	 * This does not finish the ragzip. 
	 */
	@Override
	public void flush() throws IOException {
		if(closed)
			return;
		gzos.flush();
	}
	
	private void flushPageIfFullAndOpenNew() throws IOException {
		if(pageUncompressedCount==pageMaxSize) {
			gzos.finish();
			gzos.flush();
			gzos.close(); //should be a no-op
			gzos = null;
			
			addRecord(nextPageStartOffset, 1); //into level1
			
			nextPageStartOffset = positionSupplier.getPosition();
			gzos = new GZIPOutputStream(ncos, 8192);
			pageUncompressedCount = 0;
		}
	}
	
	/**
	 * Adds an offset to an index of given level.
	 * If the level never existed, a bytebuffer is created once and reused later.
	 * 
	 * We write the level's index when its occupancy == indexMaxSize,
	 * then we record this index in the higher level (which in turn may flush and so on).
	 */
	private void addRecord(long offset, int level) throws IOException {
		if(indexes[level]==null) { //for sure levelsOccupancies[level]==0
			indexes[level] = ByteBuffer.allocate(8*indexMaxSize);
			
		} else if(levelsOccupancies[level]==indexMaxSize) {
			long indexOffset = positionSupplier.getPosition();
			
			writeEmptyGzipWithMetadata(indexes[level].flip());
			levelsOccupancies[level] = 0;
			indexes[level].clear(); //ready for reuse
			
			addRecord(indexOffset, level+1); //cascade, recursive call 
		}
		
		indexes[level].putLong(offset);
		levelsOccupancies[level]++;
	}
	
	
	/**
	 * Flush and close the internal gzip outputstream, then finished the ragzip with indexes, extensions and footer.
	 */
	@Override
	public void close() throws IOException {
		if(closed)
			return;
		
		//close last page 
		if(gzos!=null) {
			gzos.finish();
			gzos.flush();
			gzos.close(); //should be a no-op
			gzos = null;
			
			if(pageUncompressedCount>0) {
				if(indexes[1]!=null)
					addRecord(nextPageStartOffset, 1);
				pageUncompressedCount = 0;
			}
		}
		
		//---- close and write indexes ----
		
		int numberOfLevels = 0;
		long topIndexOffset = 0; //worst case of 0 levels is the 1st page offset.
		
		for(int level=1; level<indexes.length; level++) { //we don't index at level0, the page is level 0.
			if(indexes[level]==null)
				break;
			//found a partial or full level, time to flush it.
			numberOfLevels = level; //provisional watermark
			
			long indexOffset = positionSupplier.getPosition();
			topIndexOffset  = indexOffset; //provisional watermark
			
			writeEmptyGzipWithMetadata(indexes[level].flip());
			if(indexes[level+1]!=null) { //there are higher levels
				addRecord(indexOffset, level+1); //cascade, recursive call
			}
		}
		
		//---- append extensions -----
		
		long previousExtensionOffset = -1;
		if(!extensions.isEmpty()) {
			for(Extension ext: extensions) {
				if(ext.payload.length > Extension.MAX_EXT_PAYLOAD)
					continue;
				//don't throw, this could be wasting a lot of work...
				//better not add the extension silently and perhaps repair another time.
				
				long extOffset = positionSupplier.getPosition();
				ext._selfOffset = extOffset;                           //not quite necessary, but a nice gesture, in case they are inspected after write
				ext.previousExtensionOffset = previousExtensionOffset; //not quite necessary, but a nice gesture, in case they are inspected after write
				
				ByteBuffer extBB = ByteBuffer.allocate(Extension.MAX_EXT_PAYLOAD + 13); //prev8 + flags1 + id4 + payload
				extBB.putLong(ext.previousExtensionOffset);
				extBB.put(ext.flags);
				extBB.putInt(ext.id);
				extBB.put(ext.payload);
				writeEmptyGzipWithMetadata(extBB.flip());
				
				previousExtensionOffset = extOffset;
			}
		}
		
		
		//---- append footer ----
		
		ByteBuffer footerBB = ByteBuffer.allocate(100);
		footerBB.putInt(VERSION_1_0); 
		int treeSpec = (numberOfLevels<<16) | (indexSizeExponent<<8) | pageSizeExponent; //reserved, number of index levels, index size exponent, page size exponent
		footerBB.putInt(treeSpec);
		footerBB.putLong(totalUncompressed);
		footerBB.putLong(topIndexOffset); //top index offset is an index1 type, because the treeSpec said so.
		footerBB.putLong(previousExtensionOffset); //last extension offset
		footerBB.put(new byte[6]); //padding for this deflate implementation which uses 2 bytes only for an empty deflate stream.
		writeEmptyGzipWithMetadata(footerBB.flip());
		
		//---- close the underlying stream, and FileChannel by design ---
		
		ncos.getDelegate().close();
		closed = true;
	}
	
	
	private void writeEmptyGzipWithMetadata(ByteBuffer bbRA) throws IOException {
		writeEmptyGzipRAMetadata(bbRA, ncos);
	}
	
	
	//========================
	
	static class CountOS extends FilterOutputStream {
		long n;
		
		CountOS(OutputStream out) {
			super(out);
		}
		
		@Override
		public void write(byte[] ba) throws IOException {
			write(ba, 0, ba.length);
		}
		
		@Override
		public void write(byte[] ba, final int off, final int len) throws IOException {
			out.write(ba, off, len);
			n += len;
		}
		
		@Override
		public void write(int b) throws IOException {
			out.write(b);
			n++;
		}
	}
	
}