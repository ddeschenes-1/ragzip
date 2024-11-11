/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

class GzipUtils {
	
	private static final byte[] GZIP_HEADER_WITH_EXTRA = new byte[10];
	private static final byte[] GZIP_EMPTY_WITH_FOOTER = new byte[10];
	static {
		ByteBuffer bb = ByteBuffer.wrap(GZIP_HEADER_WITH_EXTRA);
		bb.putShort((short)0x1f8b);
		bb.put((byte)Deflater.DEFLATED); // compression method (8: deflate)
		bb.put((byte)(0x04)); //FLG
		bb.putInt(0);//MTIME
		bb.put((byte)0); //XFL default compression
		bb.put((byte)(255)); //OS unknown
		
		//----
		
		bb = ByteBuffer.wrap(GZIP_EMPTY_WITH_FOOTER);
		bb.putShort((short)0x0300); //empty deflate stream
		bb.putInt(0); //crc
		bb.putInt(0); //isize
	}
	
	static void writeEmptyGzipRAMetadata(ByteBuffer bbRA, OutputStream out) throws IOException {
		ByteBuffer bb = writeEmptyGzipRAMetadata(bbRA);
		out.write(bb.array(), bb.arrayOffset(), bb.remaining());
		out.flush();
	}
	
	static ByteBuffer writeEmptyGzipRAMetadata(ByteBuffer bbRA) {
		ByteBuffer bb = ByteBuffer.allocate(26 + bbRA.remaining());
		bb.put(GZIP_HEADER_WITH_EXTRA);
		
		int sflen = bbRA.remaining();
		int xlen = 4+sflen;
		bb.put((byte)(xlen & 0xff)); // little endian
		bb.put((byte)(xlen >>> 8 & 0xff));
		
		bb.put((byte)'R');
		bb.put((byte)'A');
		bb.put((byte)(sflen & 0xff));
		bb.put((byte)(sflen >>>8));
		bb.put(bbRA.array(), bbRA.arrayOffset(), sflen);
		
		bb.put(GZIP_EMPTY_WITH_FOOTER);
		return bb.flip();
	}
	
	
}
