/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectableChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.CRC32;
import java.util.zip.Deflater;


/**
 * Equivalent of the GZIPOutputStream, but with NIO apis.
 * This is NOT supporting non-blocking SelectableChannel.
 * <p>
 * The performance is slightly better due to the usage of direct ByteBuffer and possibly channel implementations
 * exploiting the FileChannel.transferTo() optimizations.
 */
public class GzipWritableChannel implements WritableByteChannel {
	private static final ByteBuffer EMPTY_BB = ByteBuffer.allocate(0);
	
	private final Deflater deflater;
	private final ByteBuffer workBuffer;
	private final CRC32 crc = new CRC32();
	private boolean closed;
	
	private final WritableByteChannel out;
	
	public GzipWritableChannel(WritableByteChannel out) throws IOException {
		this(out, Deflater.DEFAULT_COMPRESSION);
	}
	
	/**
	 * @param compressionLevel 
	 * @see Deflater#DEFAULT_COMPRESSION 
	 * @see Deflater#BEST_COMPRESSION 
	 * @see Deflater#NO_COMPRESSION 
	 */
	public GzipWritableChannel(WritableByteChannel out, int compressionLevel) throws IOException {
		this.out = out;
		if (out instanceof SelectableChannel && !((SelectableChannel)out).isBlocking())
			throw new IllegalArgumentException("SelectableChannel in non-blocking mode not supported");
		
		this.deflater = new Deflater(compressionLevel, true);
		this.deflater.setStrategy(Deflater.DEFAULT_STRATEGY);
		this.workBuffer = ByteBuffer.allocateDirect(0xffff + 2);
		writeHeader(compressionLevel);
		workBuffer.clear();
	}
	
	@Override
	public boolean isOpen() {
		return !closed;
	}
	
	private void writeHeader(int compressionLevel) throws IOException {
		workBuffer.clear();
		workBuffer.put((byte)0x1f); //ID1
		workBuffer.put((byte)0x8b); //ID2
		workBuffer.put((byte)Deflater.DEFLATED); //CM
		workBuffer.put((byte)0); //FLG
		workBuffer.putInt(0); //MTIME
		workBuffer.put((byte)switch (compressionLevel) { //XFL
			case Deflater.BEST_COMPRESSION -> 2;
			case Deflater.BEST_SPEED -> 4;
			default -> 0;
		});
		
		workBuffer.put((byte)255); //OS unknown
		workBuffer.flip();
		while(workBuffer.hasRemaining())
			out.write(workBuffer);
	}
	
	@Override
	public int write(ByteBuffer src) throws IOException {
		if (deflater.finished())
			throw new IOException("compression already finished");
		
		int len = src.remaining();
		if (len > 0) {
			src.mark();
			deflater.setInput(src);
			while (!deflater.needsInput())
				drainDeflaterOnce();
			
			///////////////////////////=
			//CRITICAL shield against mutations, like FileChannel.transferTo() doing a clear!
			//the deflater hold the buffer reference and its state is important for any subsequent
			//call to .deflate()
			//By setting an empty ByteBuffer, we avoid repeating the data (valid or not) of that last buffer.
			deflater.setInput(EMPTY_BB);
			////////////////////////////
			
			src.reset();
			crc.update(src);
		}
		return len;
	}
	
	private void drainDeflaterOnce() throws IOException {
		workBuffer.clear();
		deflater.deflate(workBuffer);
		workBuffer.flip();
		while (workBuffer.hasRemaining()) //this is only efficient on blocking channel, else may spin fast...
			out.write(workBuffer);
	}
	
	
	@Override
	public void close() throws IOException {
		if (closed)
			return;
		try {
			finish();
		} finally {
			deflater.end();
			out.close();
			closed = true;
		}
	}
	
	/**
	 * @see java.util.zip.GZIPOutputStream#finish()
	 */
	public void finish() throws IOException {
		if (deflater.finished())
			return;
		deflater.finish();
		while (!deflater.finished())
			drainDeflaterOnce();
		
		workBuffer.clear().order(ByteOrder.LITTLE_ENDIAN);
		workBuffer.putInt((int)crc.getValue());
		workBuffer.putInt(deflater.getTotalIn());
		workBuffer.flip();
		while(workBuffer.hasRemaining())
			out.write(workBuffer);
	}
	
}
