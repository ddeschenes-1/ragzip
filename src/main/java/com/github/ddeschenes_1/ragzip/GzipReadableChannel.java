package com.github.ddeschenes_1.ragzip;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

/**
 * Equivalent of GZIPInputStream but using NIO's ReadableByteChannel.
 * @see java.util.zip.GZIPInputStream
 */
public class GzipReadableChannel implements ReadableByteChannel {
	private static final int FEXTRA = 4;    // Extra field
	private static final int FNAME = 8;    // File name
	private static final int FCOMMENT = 16;   // File comment
	private static final int FHCRC = 2;    // Header CRC
	
	private Inflater inf;
	private CRC32 crc = new CRC32();
	
	private ByteBuffer inputBuffer;
	protected boolean eos;
	private boolean closed;
	private ReadableByteChannel srcChannel;
	boolean concatenated;
	
	/**
	 * calls GzipReadableChannel(in, true).
	 */
	public GzipReadableChannel(ReadableByteChannel in) throws IOException {
		this(in, true);
	}
	
	/**
	 * calls GzipReadableChannel(in, true, 0).
	 */
	public GzipReadableChannel(ReadableByteChannel in, boolean concatenated) throws IOException {
		this(in, concatenated, 0);
	}
	
	/**
	 * @param in the source channel positioned at the start of a gzip member.
	 * @param concatenated when false, the decompression stops at the end of the initial gzip member; when true, the decompression continues to the end of the file. 
	 * @param preload when>0, the input channel will be read by this amount before reading the gzip member; otherwise, a full buffer of 8192 will be loaded during header parsing.
	 * 	   This is expected to avoid some unnecessary read from the source, when the amount of bytes to read or skip is small.
	 * 	   Note that Gzip headers are at least 10 bytes, and an empty gzip with the trailer is 20 bytes.
	 */
	public GzipReadableChannel(ReadableByteChannel in, boolean concatenated, int preload) throws IOException {
		this.srcChannel = in;
		this.concatenated = concatenated;
		
		if (in instanceof SelectableChannel && !((SelectableChannel)in).isBlocking())
			throw new IllegalArgumentException("SelectableChannel in non-blocking mode not supported");
		
		this.inputBuffer = ByteBuffer.allocate(8192); //.flip to make it empty on start
		
		if(preload>0) {
			this.inputBuffer.limit(Math.min(8192, preload));
			int bytesRead = srcChannel.read(inputBuffer);
			if (bytesRead<0) {
				eos = true;
				throw new EOFException();
			}
		}
		inputBuffer.flip();
		
		this.inf = new Inflater(true);
		readHeader();
	}
	
	private void readHeader() throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN);
		srcReadFully(bb);
		bb.flip();
		
		// Check header magic
		if (bb.getChar() != 0x8b1f)
			throw new ZipException("Not in GZIP format");
		if (bb.get() != 8)
			throw new ZipException("Unsupported compression method");
		int flg = bb.get() & 0xff;
		int mtime = bb.getInt();
		int xfl = bb.get() & 0xff;
		int os = bb.get() & 0xff;
		
		boolean hcrc = (flg & FHCRC) !=0;
		if(hcrc) {
			crc.reset();
			crc.update(bb.rewind());
		}
		
		//-------
		
		if ((flg & FEXTRA) != 0) {
			srcReadFully(bb.clear().limit(2));
			if(hcrc)
				crc.update(bb.flip());
			int xlen = bb.flip().getChar();
			ByteBuffer extraBB = ByteBuffer.allocate(xlen);
			srcReadFully(extraBB);
			if(hcrc)
				crc.update(extraBB.flip());
		}
		
		if ((flg & FNAME) != 0)
			readUntil_0(hcrc);
		
		if ((flg & FCOMMENT) != 0)
			readUntil_0(hcrc);
		
		if (hcrc) {
			srcReadFully(bb.clear().limit(2));
			int exp = bb.flip().getChar();
			int act = (int)(crc.getValue() & 0xffff);
			if(exp!=act)
				 throw new ZipException("Corrupt GZIP header");
		}
		crc.reset();
		inf.reset();
		inf.setInput(inputBuffer);
	}
	
	
	
	//----------
	
	@Override
	public boolean isOpen() {
		return !closed;
	}
	
	@Override
	public int read(ByteBuffer dst) throws IOException {
		if (closed)
			throw new IOException("Stream closed");
		if (eos)
			return -1;
		try {
			int pos = dst.position();
			int limit = dst.limit();
			
			int n;
			while( (n = inf.inflate(dst)) == 0) {
				if(inf.finished() || inf.needsDictionary()) {
					//deflater stream is finished
					readFooter();
					
					if(!concatenated)
						return -1;
					
					//but is there another gzip member ?
					try {
						readHeader();
					} catch(IOException e) {
						return -1;
					}
				}
				
				if(inf.needsInput()) {
					if(!inputBuffer.hasRemaining()) {
						inputBuffer.clear();
						int len;
						
						//THIS IS THE REFILL
						
						//NICETOHAVE: We might want the 1st read to only read ~= (skipping distance + dst.remaining()) / compressionratio.
						//We have that compression ratio from inflater bytes read vs written, assuming its avg is fair (might need to be bounded).
						//The 8192 bytes inputbuffer could easily cover more than one concatenated gzip member (small ragzip pages < 8kB compressed),
						//so the inflater ratio will jump suddenly as it changes, and may be undefined. A smooted avg tracker would be better.
						//
						//The envisioned improvement is to implement skipNBytes(wasteQty, wantedQty) which would go so far up
						//in all read/transferto flavors that only the buffer-less position(long) would have to guess the wantedQty.
						//
						//For all other read than the 1st read, it is usually less efficient to fillup less than the full inputbuffer
						//because it implies more calls to underlying srcChannel. We only wanted to spare a massive load on gzip init.
						
						while( (len=srcChannel.read(inputBuffer)) == 0) {//
						}
						if(len<0) {
							eos = true;
							throw new EOFException("Unexpected end of ZLIB input stream");
						}
						inputBuffer.flip();
					}
					inf.setInput(inputBuffer);
				}
			}
			
			crc.update(dst.flip().position(pos));
			dst.limit(limit);
			return n;
		} catch (DataFormatException e) {
			throw new IOException("Invalid ZLIB data format at input byte "+inf.getBytesWritten()+", 0x"+Long.toHexString(inf.getBytesWritten())+"", e);
		}
	}
	
	private void readFooter() throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
		srcReadFully(bb); //pull whatever might be missing
		bb.flip();
		int expectedCrc = bb.getInt();
		int expectedISize = bb.getInt();
	        if ( (expectedCrc != (int)crc.getValue())
	        	|| (expectedISize != (int)inf.getBytesWritten()))
	                throw new ZipException("Corrupt GZIP trailer");
	}
	
	@Override
	public void close() throws IOException {
		if (closed)
			return;
		inf.end();
		srcChannel.close();
		closed = true;
	}
	
	
	//------------------
	
	private String readUntil_0(boolean doCrc) throws IOException {
		if(eos)
			throw new EOFException();
		
		byte[] dstBytes = new byte[0x10000];//64kB max (0xffff without the \0)
		ByteBuffer dstBB = ByteBuffer.wrap(dstBytes);
		
		int lastCheckedPos = -1;
		while(lastCheckedPos < dstBytes.length-1) {
			_refillIfNeeded();
			final int qty = Math.min(inputBuffer.remaining(), dstBB.remaining());
			final int inputPos = inputBuffer.position();
			
			dstBB.put(inputBuffer.slice(inputPos, qty));
			int consumed = 0;
			try {
				while(consumed < qty && lastCheckedPos < dstBytes.length-1) {
					consumed++;
					lastCheckedPos++;
					if(dstBytes[lastCheckedPos]==0) {//found null terminator
						if(doCrc) {
							crc.update(dstBytes, 0, lastCheckedPos);
							crc.update(0);
						}
						return new String(dstBytes, 0, lastCheckedPos, StandardCharsets.ISO_8859_1);
					}
				}
			} finally {
				inputBuffer.position(inputPos + consumed);
			}
		}
		
		throw new IOException("nul-terminated string exceeds 0xffff (65535) bytes. Aborting.");
	}
	
	void srcReadFully(ByteBuffer dst) throws IOException {
		if(eos)
			throw new EOFException();
		
		while(dst.hasRemaining()) {
			_refillIfNeeded();
			int qty = Math.min(inputBuffer.remaining(), dst.remaining());
			int ipos = inputBuffer.position();
			dst.put(inputBuffer.slice(ipos, qty));
			inputBuffer.position(ipos+qty);
		}
	}
	
	private void _refillIfNeeded() throws IOException {
		if (inputBuffer.hasRemaining())
			return;
		
		//this is the refill, and it is not smart...
		//there is no hint to load less bytes because it is in the early gzip bytes or such.
		inputBuffer.clear();
		int bytesRead = srcChannel.read(inputBuffer);
		if (bytesRead<0) {
			eos = true;
			throw new EOFException();
		}
		inputBuffer.flip();
	}
	
	/**
	 * This skip uses this.read() and will update the crc.
	 */
	void skipNBytes(long distance) throws IOException {
		if(distance<0x10000) {
			ByteBuffer waste = ByteBuffer.allocate((int)distance);
			Utils.readFully(this, waste, () -> "EOF");
		} else {
			ByteBuffer waste = ByteBuffer.allocate(0x10000);
			while(distance>0) {
				waste.clear();
				if(distance < 0x10000) {
					waste.limit((int)distance);
					Utils.readFully(this, waste, () -> "EOF");
					return;
				}
				Utils.readFully(this, waste, () -> "EOF");
				distance -= 0x10000;
			}
		}
	}
	
}
