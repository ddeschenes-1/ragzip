/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;

class ExtraField {
	static class SubField {
		byte si1, si2;
		ByteBuffer payload;
		SubField(byte si1, byte si2, ByteBuffer payload) {
			this.si1 = si1;
			this.si2 = si2;
			this.payload = payload;
		}
	}
	
	static ExtraField fromGzipHead(SeekableByteChannel ch) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(12);
		Utils.readFully(ch, bb, () -> "EOF while reading gzip header");
		
		bb.flip();
		int magic = bb.getShort() & 0xffff;
		int cm = bb.get() & 0xff;
		int flg = bb.get() & 0xff;
		int mtime = bb.getInt();
		int xfl = bb.get() & 0xff;
		int os = bb.get() & 0xff;
		
		if(magic!=0x1f8b || cm!=0x08 || (flg&0xe0)!=0)
			throw new IOException("does not appear to be gzip");
		
		if((flg & 0x04) ==0) //no extra
			return null;
		
		ExtraField e = new ExtraField();
		
		int xlen = (bb.get()&0xff) | ((bb.get() & 0xff)<<8); //little endian
		if(xlen<=0)
			return e;
		
		
		//--- read subfields -----
		
		bb = ByteBuffer.allocate(xlen).order(ByteOrder.LITTLE_ENDIAN);
		Utils.readFully(ch, bb, () -> "EOF while reading extra (of "+xlen+"bytes)");
		bb.flip();
		while(bb.hasRemaining()) {
			byte si1 = bb.get();
			byte si2 = bb.get();
			int sflen = bb.getChar();
			if (sflen > bb.remaining())
				throw new IOException("ExtraField subfield lenght exceeds remaining bytes in extra: " + sflen + " > " + bb.remaining());
			ByteBuffer payload = bb.slice(bb.position(), sflen);
			bb.position(bb.position() + sflen);
			e.subFields.add(new SubField(si1, si2, payload));
			e.encodedSize = bb.position();
		}
		
		if (bb.hasRemaining())
			throw new IOException("" + bb.remaining() + " remaining bytes not used to parse an extra subfield.");
		
		return e;
	}
	
	//========================
	
	ArrayList<SubField> subFields = new ArrayList<>();
	int encodedSize = 0;
	
	ExtraField() {
	}
	
	SubField findFirstSubField(char si1, char si2) {
		for(int i=0, n=subFields.size(); i<n; i++) {
			SubField f = subFields.get(i);
			if(f.si1==(byte)si1 && f.si2==(byte)si2)
				return f;
		}
		return null;
	}
	
}
