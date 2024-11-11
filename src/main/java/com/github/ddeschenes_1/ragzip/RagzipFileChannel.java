/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import static com.github.ddeschenes_1.ragzip.Utils.readFully;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Consumer;

import com.github.ddeschenes_1.ragzip.ExtraField.SubField;
import com.github.ddeschenes_1.ragzip.Utils.NoCloseChannel;


/**
 * Allow reading a ragzip content as if it is an ordinary FileChannel, 
 * virtualizing the uncompressed content and implementing random-access seeking.
 * <p>
 * Note that it can work over any SeekableByteChannel, not just FileChannel, enabling the use of implementations
 * like over http bytes ranges.
 * <p>
 * This implementation does not cache pages. See {@link CachingSeekableReadByteChannel} for that support.
 * <p>
 * When a seek to a position is within the current page, and at of after the current uncompressed position,
 * and the method is one that will alter the position of the channel (not those with a position arg),
 * then there will not be any index tree navigation and the channel will simply skip ahead to the requested byte.
 * This is the fast forward path.
 */
public final class RagzipFileChannel extends FileChannel {
	
	/**
	 * When this cache size (-1) is specified, the index are read directly from the file without opening a metadata gzip member.
	 * This is typically much faster than CACHE_SIZE_CACHELESS_LOADED (0).
	 * 
	 * No memory is consumed to cache any index so the lookup time involves all the index tree navigation.
	 */
	public static final short CACHE_SIZE_CACHELESS_DIRECT = -1;
	
	
	/**
	 * When this cache size (0) is specified, the index are read by loading the metadata gzip member.
	 * This is the slowest mode but it is provided in case of incompatibility with a typical empty gzip member.
	 * The larger the indexes, the more wasted bytes are read.
	 * 
	 * No memory is consumed to cache any index and the lookup time involves all the index tree navigation.
	 */
	public static final short CACHE_SIZE_CACHELESS_LOADED = 0;
	
	
	public static final int VERBOSITY_QUIET = 0;
	public static final int VERBOSITY_CACHE = 1;
	public static final int VERBOSITY_INIT = 2;
	public static final int VERBOSITY_TRACING = 3;
	
	
	final SeekableByteChannel srcChannel; //direct source of bytes 
	final InputStream srcInputStream; //direct stream in bidirectional sync with the srcChannel
	int verbosity = 0;
	Consumer<String> logger = System.out::println;
	
	int version;
	long logicalSize;
	byte numberOfLevels;
	byte pageSizeExponent; //ex: 13
	byte indexSizeExponent;//ex: 12
	long pageOffsetMask; //ex: 0x0000_0000_0000_ffff for 2^16 = 64kB pages
	long indexOffsetMask;//ex: 0x0000_0000_0000_0fff for 2^12 = 4k index
	
	long topIndexOffset;
	long extensionsOffset;
	
	final LinkedList<Extension> extensions = new LinkedList<>();
	
	long uncompressedCurrentPosition;
	
	
	/**
	 * The outmost decompressed source of bytes wrapping the NoCloseChannel which is wrapping the srcChannel.
	 */
	GzipReadableChannel gzrc;
	
	boolean useCache = false;
	int cacheSizePerLevel = CACHE_SIZE_CACHELESS_DIRECT;
	Map<Long,ByteBuffer>[] indexesCache;
	
	
	//=========================
	
	/**
	 * @see RagzipFileChannel#RagzipFileChannel(SeekableByteChannel, int, int)
	 */
	public RagzipFileChannel(FileChannel ragzipFC) throws IOException {
		this(ragzipFC, VERBOSITY_QUIET, CACHE_SIZE_CACHELESS_DIRECT);
	}
	
	/**
	 * @param ragzipFC the file channel on the ragzip file.
	 * @param verbosity 0 is mute, 1 is minimal on open, 2 includes cache events, 3 is completely nuts (all the seeking).
	 * @param cacheSizePerLevel when positive, is the number of index to keep in memory at each level.
	 * 		0 loads the index but doesn't cache it (slowest, should not be used except for full validation),
	 * 		-1 is the cacheless mode direct with direct seek on gz member.
	 * 		The maximum value is a matter of memory. This implementation will limit it to 65535 (0xffff).
	 */
	@SuppressWarnings({ "serial", "unchecked" })
	public RagzipFileChannel(SeekableByteChannel ragzipFC, int verbosity, int cacheSizePerLevel) throws IOException {
		this.srcChannel = ragzipFC;
		this.srcInputStream = Channels.newInputStream(srcChannel);
		this.verbosity = verbosity;
		this.cacheSizePerLevel = cacheSizePerLevel;
		if(cacheSizePerLevel<CACHE_SIZE_CACHELESS_DIRECT)
			throw new IllegalArgumentException("cacheSizePerLevel must be >= -1");
		if(cacheSizePerLevel>0xffff)
			throw new IllegalArgumentException("cacheSizePerLevel must be <= 0xffff");
		
		loadFooter();
		loadExtensions();
		
		useCache = cacheSizePerLevel > 0;
		if(useCache) {
			indexesCache = new Map[numberOfLevels+1]; //[0] never used
			for(int i=1; i<=numberOfLevels; i++) {
				int level = i;
				indexesCache[i] = new LinkedHashMap<>(16, 0.25f, true) {
					@Override
					protected boolean removeEldestEntry(Map.Entry<Long,ByteBuffer> eldest) {
						boolean evict = size() > cacheSizePerLevel;
						if(RagzipFileChannel.this.verbosity >= VERBOSITY_CACHE && evict)
							log("evicting cached index level "+level
								+ " #"+Long.toHexString(eldest.getKey())
								+" [logical start 0x"+Long.toHexString(eldest.getKey()<<(level*indexSizeExponent))+"]"
							);
						return evict;
					}
				};
			}
		}
		
		srcChannel.position(0); //no seek needed for 1st page, but doesn't hurt to be reminded.
		uncompressedCurrentPosition=0;
		gzrc = new GzipReadableChannel(new NoCloseChannel(srcChannel));
	}
	
	/**
	 * Set a consumer of string messages when used in verbose modes.
	 */
	public void setLogger(Consumer<String> logger) {
		this.logger = logger!=null ? logger : System.out::println;
	}
	
	private void log(Object msg) {
		logger.accept(""+msg);
	}
	
	ByteBuffer unwrapMetadataPayload() throws IOException {
		ExtraField ef = ExtraField.fromGzipHead(srcChannel);
		if(ef!=null) {
			SubField sf = ef.findFirstSubField('R','A');
			if(sf!=null)
				return sf.payload;
		}
		
		throw new IOException("gzip extra subfield 'RA' not found");
	}
	
	void loadFooter() throws IOException {
		final long footerOffset = srcChannel.size() - RagzipOutputStream.FOOTER_LENGTH64;
		srcChannel.position(footerOffset);
		
		//gzip is:
		//id1 id2 cm flg   mtime    xfl os = 10
		//+2 emptydeflate = 12
		//+4 crc32 = 16
		//+4 isize = 20
		
		//+6 xlen2 si1 si2 sflen2 = 26
		//+38 footer = 64
		ByteBuffer bb = ByteBuffer.allocate(64);
		readFully(srcChannel, bb, () -> "Unabled to read the 64 bytes footer (missing "+bb.remaining()+" bytes only)");
		
		bb.flip();
		int start = bb.getInt();
		if(start != 0x1f8b0804)
			throw new IOException("Ragzip footer does not start like a gzip member with FEXTRA (0x1f8b0804): seen 0x"+Long.toHexString(start & 0xffffFFFFL | 0x1_00000000L).substring(1));
		bb.getLong(); //mtime xfl, os, XLEN
		short sfid = bb.getShort(); //si1 si2
		if(sfid != 0x5241)
			throw new IOException("Ragzip footer does not have subfield 'RA' (0x5241): seen 0x"+Integer.toHexString(sfid & 0xFFFF | 0x10000).substring(1));
		
		//--- probably safe to read as gzip now.
		srcChannel.position(footerOffset);
		ByteBuffer footerBB = unwrapMetadataPayload();
		if(verbosity>=VERBOSITY_INIT)
			log("Footer payload length: "+footerBB.remaining());
		
		version = footerBB.getInt();
		int treeSpec = footerBB.getInt();
		logicalSize = footerBB.getLong();
		topIndexOffset = footerBB.getLong();
		extensionsOffset = footerBB.getLong();
		
		pageSizeExponent = (byte)(treeSpec & 0xff);
		indexSizeExponent = (byte)((treeSpec>>>8) & 0xff);
		numberOfLevels = (byte)((treeSpec>>>16) & 0xff);
		int reserved = (treeSpec>>>24) & 0xff;
		
		pageOffsetMask = (1L<<pageSizeExponent) -1;
		indexOffsetMask = (1L<<indexSizeExponent) -1;

		if(numberOfLevels>0 && topIndexOffset>=footerOffset)
			throw new IOException("topindex offset suggested (0x"+Long.toHexString(topIndexOffset)+")"
				+ " is not before footer offset (0x"+Long.toHexString(footerOffset)+"). Possible footer corruption.");
		if(extensionsOffset>=footerOffset) //could be -1, which is still ok for this check
			throw new IOException("extensions offset suggested (0x"+Long.toHexString(extensionsOffset)+")"
				+ " is not before footer offset (0x"+Long.toHexString(footerOffset)+"). Possible footer corruption.");
		
		
		
		if(verbosity>=VERBOSITY_INIT) {
			log("version "+(version>>>16)+"."+(version&0xffff)+" (0x"+Integer.toHexString(version)+")");
			log("tree spec: 0x"+Long.toHexString(0x1_0000_0000L | treeSpec&0xffff_ffffL).substring(1));
			log("   reserved = "+Integer.toHexString(reserved));
			log("   levels   = "+numberOfLevels);
			log("   idx sz   = 2^"+indexSizeExponent + " ("+(1<<indexSizeExponent)+")");
			log("   pg sz    = 2^"+pageSizeExponent +" ("+(1<<pageSizeExponent)+")");
			log("uncompressed size: "+logicalSize+ " (0x"+Long.toHexString(logicalSize)+")");
			log("topIndexOffset        : 0x"+Long.toHexString(topIndexOffset));
			log("extensions list offset: 0x"+Long.toHexString(extensionsOffset));
			log("padding: "+footerBB.remaining()+" bytes");
			
			if(logicalSize>0) {
				long items = ((logicalSize-1) >>> pageSizeExponent)+1;
				log("derived number of pages: "+items);
				for(int level=1; level<=numberOfLevels && items>0; level++) {
					items = ((items-1)>>>indexSizeExponent)+1;
					log("derived number of level "+level+" indexes: "+items);
				}
			}
		}
		
		if(version!=RagzipOutputStream.VERSION_1_0)
			throw new IOException("Unsupported version 0x"+Integer.toHexString(version));
		if(numberOfLevels>53)
			throw new IOException("Unsupported number of levels: "+numberOfLevels);
		if(pageSizeExponent<9 || pageSizeExponent>30 || indexSizeExponent<1 || indexSizeExponent>12)
			throw new IOException("Unsupported tree spec: 0x"+Integer.toHexString(treeSpec));
	}
	
	
	void loadExtensions() throws IOException {
		long extOffset = extensionsOffset;
		//---extensions---
		
		while(extOffset>=0 && extensions.size()<Extension.MAX_EXT_COUNT) {
			srcChannel.position(extOffset);
			ByteBuffer extBB = unwrapMetadataPayload();
			
			Extension ext = new Extension();
			ext._selfOffset = extOffset;
			
			long previousOffset = extBB.getLong();
			if(previousOffset>=extOffset)
				throw new IOException("offset suggested (0x"+Long.toHexString(previousOffset)+")"
					+ " is not before current extension offset (0x"+Long.toHexString(extOffset)+"). Possible extension linked list corruption.");
			ext.previousExtensionOffset = extOffset = previousOffset;
			ext.flags = extBB.get();
			ext.id = extBB.getInt();
			int sflen = extBB.remaining();
			if(sflen>Extension.MAX_EXT_PAYLOAD) {
				if(verbosity>=VERBOSITY_INIT)
					log("Ignored extension of length over "+Extension.MAX_EXT_PAYLOAD+" (sflen="+sflen+")");
				continue;
			}
			ext.payload = new byte[sflen];
			extBB.get(ext.payload);
			
			extensions.addFirst(ext); //restore original order
			
			if(verbosity>=VERBOSITY_INIT)
				log("found "+ext);
		}
		
		if(extOffset>=0 && verbosity>0)
			log("Further extensions were not loaded because there are already "+Extension.MAX_EXT_COUNT+" of them.");
	}
	
	
	private GzipReadableChannel _seekTo(long logicalPos, boolean updatePosition) throws IOException {
		if(logicalPos<0)
			throw new IOException("logical pos < 0");
		if(logicalPos>=logicalSize)
			throw new IOException("logical pos > logicalSize ("+logicalSize+")");
		
		long newPageId = logicalPos >>> pageSizeExponent;
		
		if(verbosity>=VERBOSITY_TRACING) {
			log("logicalPos "+logicalPos);
			log("level 0 position (logical offset in page): "+(logicalPos & pageOffsetMask));
		}
		
		if(updatePosition) {
			if(logicalPos==uncompressedCurrentPosition) {
				if(verbosity>=VERBOSITY_TRACING)
					log("same position detected, nothing to change");
				return gzrc;
			}
			
			long currentPageId = uncompressedCurrentPosition >>> pageSizeExponent;
			
			if(newPageId==currentPageId && uncompressedCurrentPosition<logicalPos) {
				//same page, continue skipping
				long distance = logicalPos - uncompressedCurrentPosition;
				if(verbosity>=VERBOSITY_TRACING)
					log("same page detected and distance is ahead by "+distance);
				this.gzrc.skipNBytes(distance);
				this.uncompressedCurrentPosition = logicalPos;
				return gzrc;
			}
		}
		
		////////////////////////////////////////////////
		GzipReadableChannel gz = _roughtSeek(newPageId);
		////////////////////////////////////////////////
		
		if(updatePosition)
			this.uncompressedCurrentPosition = newPageId << pageSizeExponent; //temporary
		
		long toSkip = logicalPos & pageOffsetMask;
		gz.skipNBytes(toSkip);
		
		if(updatePosition) {
			//if not detached, we update the state
			this.gzrc.close();
			this.gzrc = gz;
			this.uncompressedCurrentPosition = logicalPos;
		}
		
		return gz;
	}
	
	
	GzipReadableChannel _roughtSeek(long targetPageId) throws IOException {
		//Ex: 64 bits logical offset (12 bits per pages and indexes) was ...0000IIIiiipppxxx
		//The target page id is without the page offset x bits:  ...0000IIIiiippp
		//level1 index uses slotNumberByLevel[1] = ppp to lookup the page,   but its level idxkey is ...000IIIiii   (shifted out 1 level, got rid of ppp)
		//level2 index uses slotNumberByLevel[2] = iii to lookup the level1, but its level idxkey is ...000III      (shifted out 2 level, got rid of iiippp)
		
		int[] slotNumberByLevel = new int[numberOfLevels+1]; //level 0 is the page (unused here), level 1..n for indexes
		
		long slotsBits = targetPageId;
		for(int i=1; i<=numberOfLevels; i++) {
			slotNumberByLevel[i] = (int)(slotsBits & indexOffsetMask);
			if(verbosity>=VERBOSITY_TRACING)
				log("  level "+i+" position: "+slotNumberByLevel[i]);
			slotsBits >>>= indexSizeExponent;
		}
		
		long currentOffset = topIndexOffset;
		for(int i=numberOfLevels; i>0; i--) {
			
			if(cacheSizePerLevel==CACHE_SIZE_CACHELESS_DIRECT) {
				//Special "lean" cacheless mode
				//Load the long from gzoffset + 16 + 8*slotNumberByLevel[i]
				//The 16 is from the 10 bytes gzip header + 2 bytes for XLEN + 2 bytes for si1_si2 + 2 bytes for subfield length
				//This does not assert that the subfield is 'RA'; doing so would require a near seek at +12 and another at +16+*slotNumberByLevel[i]
				//We presume that if we got the footer right, this is proper ragzip metadata.
				
				long pos = currentOffset + 16 + (slotNumberByLevel[i]<<3);
				srcChannel.position(pos);
				ByteBuffer bb = ByteBuffer.allocate(8);
				readFully(srcChannel, bb, () -> "failing reading 8 bytes from 0x"+Long.toHexString(pos));
				bb.flip();
				long newOffset = bb.getLong();
				if(newOffset>=currentOffset)
					throw new IOException("offset suggested (0x"+Long.toHexString(newOffset)+")"
						+ " is not before current offset (0x"+Long.toHexString(currentOffset)+"). Possible index corruption.");
				currentOffset = newOffset;
			} else {
				//load index of level i
				Long idxKey = targetPageId >>> (i*indexSizeExponent);
				ByteBuffer indexBB = useCache ? indexesCache[i].get(idxKey) : null;
				if(indexBB==null) {
					//cache miss or not caching
					srcChannel.position(currentOffset);
					indexBB = unwrapMetadataPayload();
					if(useCache) {
						if(verbosity>=VERBOSITY_CACHE)
							log("caching index level "+i
								+ " #"+Long.toHexString(idxKey)
								+" [logical start 0x"+Long.toHexString(idxKey<<(i*indexSizeExponent))+"]"
							);
						indexesCache[i].put(idxKey, indexBB); //not using map.computeIfAbsent() because wrapping IOExceptions into RuntimeExceptions makes me cry. 
					}
				}
				long newOffset =indexBB.getLong(slotNumberByLevel[i]<<3);//ex: the 3rd long, at slot number 2, is located at 8*2=16 bytes from index start.
				if(newOffset>=currentOffset)
					throw new IOException("offset suggested (0x"+Long.toHexString(newOffset)+")"
						+ " is not before current offset (0x"+Long.toHexString(currentOffset)+"). Possible index corruption.");
				currentOffset = newOffset;
			}
		}
		
		if(verbosity>=VERBOSITY_TRACING)
			log("page start offset: "+currentOffset);
		
		//---seek page start position --
		
		srcChannel.position(currentOffset);
		GzipReadableChannel gc = new GzipReadableChannel(new NoCloseChannel(srcChannel)); //ordinary gzip input stream would do fine here.
		return gc;
	}
	

	//====== public api ===============
	
	/**
	 * The (hex) MMMMmmmm version found in the footer.
	 */
	public int getVersion() {
		return version;
	}
	/**
	 * The number of levels found in the footer.
	 */
	public byte getNumberOfLevels() {
		return numberOfLevels;
	}
	/**
	 * The index size exponent found in the footer.
	 */
	public byte getIndexSizeExponent() {
		return indexSizeExponent;
	}
	/**
	 * The page size exponent found in the footer.
	 */
	public byte getPageSizeExponent() {
		return pageSizeExponent;
	}
	
	/**
	 * The extensions found in the footer.
	 */
	public LinkedList<Extension> getExtensions() {
		return extensions;
	}
	
	
	
	//======= filechannel api ===========
	
	@Override
	public int read(ByteBuffer dst) throws IOException {
		int n = gzrc.read(dst);
		if(n<0)
			return -1;
		this.uncompressedCurrentPosition += n;
		return n;
	}

	@Override
	public int read(ByteBuffer dst, long position) throws IOException {
		long fcPositionbackup = srcChannel.position();
		try (GzipReadableChannel gz = _seekTo(position, false)) {
			return gz.read(dst);
		} finally {
			srcChannel.position(fcPositionbackup);
		}
	}
	
	@Override
	public long read(ByteBuffer[] dsts, int off, int len) throws IOException {
		final int end = off+len;
		if ((off | len | end | (dsts.length - end)) < 0)
			throw new IndexOutOfBoundsException();
		int total = 0;
		int qty;
		for(int i=off; i<end; i++) {
			qty = read(dsts[i]);
			if(qty<0)
				break;
			total += qty;
		}
		return total;
	}
	
	@Override
	public long transferTo(long logicalPos, long count, WritableByteChannel target) throws IOException {
		long fcPositionbackup = srcChannel.position();
		
		try (GzipReadableChannel gz = _seekTo(logicalPos, false)) {
			long total = 0;
			ByteBuffer bb = ByteBuffer.allocate((int)Math.min(count, 8192));
			
			//then write the count amount (or less) to target
			while(count>0) {
				int len = (int)Math.min(count, bb.capacity());
				bb.clear().limit(len);
				int qty = gz.read(bb);
				if(qty<0)
					break;
				target.write(bb.flip());
				total += qty;
				count -= qty;
			}
			
			return total;
		} finally {
			srcChannel.position(fcPositionbackup);
		}
	}
		

	@Override
	public long position() throws IOException {
		return this.uncompressedCurrentPosition;
	}

	@Override
	public FileChannel position(long logicalPos) throws IOException {
		_seekTo(logicalPos, true);
		return this;
	}

	/**
	 * Return the total uncompressed (logical) size contained in the ragzip file. This is typically larger than the file on storage. 
	 */
	@Override
	public long size() throws IOException {
		return logicalSize;
	}

	@Override
	protected void implCloseChannel() throws IOException {
		if(gzrc!=null)
			gzrc.close();
		gzrc = null;
		srcChannel.close();
	}
	
	
	/**
	 * Throws NonWritableChannelException (unless the srcChannel is a FileChannel, where teh call is forwarded)
	 */
	@Override
	public void force(boolean metaData) throws IOException {
		if(srcChannel instanceof FileChannel fc)
			fc.force(metaData); //even a read-only may write (last access time for example)
		else
			throw new NonWritableChannelException();
	}
	
	
	//------- NOT SUPPORTED ------------
	
	/**
	 * Throws NonWritableChannelException.
	 */
	@Override
	public int write(ByteBuffer src) throws IOException {
		throw new NonWritableChannelException();
	}
	/**
	 * Throws NonWritableChannelException.
	 */
	@Override
	public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
		throw new NonWritableChannelException();
	}
	/**
	 * Throws NonWritableChannelException.
	 */
	@Override
	public FileChannel truncate(long size) throws IOException {
		throw new NonWritableChannelException();
	}
	/**
	 * Throws NonWritableChannelException.
	 */
	@Override
	public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
		throw new NonWritableChannelException();
	}
	/**
	 * Throws NonWritableChannelException.
	 */
	@Override
	public int write(ByteBuffer src, long position) throws IOException {
		throw new NonWritableChannelException();
	}
	/**
	 * Throws NonWritableChannelException.
	 */
	@Override
	public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
		throw new UnsupportedOperationException();
	}
	/**
	 * Throws NonWritableChannelException.
	 */
	@Override
	public FileLock lock(long position, long size, boolean shared) throws IOException {
		throw new UnsupportedOperationException();
	}
	/**
	 * Throws NonWritableChannelException.
	 */
	@Override
	public FileLock tryLock(long position, long size, boolean shared) throws IOException {
		throw new UnsupportedOperationException();
	}
	
}
