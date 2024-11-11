/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import java.nio.charset.StandardCharsets;

/**
 * Used to add custom or spec extensions to the footer.
 * Classes external to the package cannot make spec extensions; only the implementation does.
 * <p>
 * Therefore, any attempt to set the SPEC bit (0x80) in the flags by a class outside the package will fail.
 */
public class Extension {
	public static final byte FLAG_SPEC = (byte)0x80;
	//public static final int MAX_EXT_PAYLOAD = 0xffff - 4 - 13; //xlen max - [sfid2,sflen2, long8, byte1, int4] 
	public static final int MAX_EXT_PAYLOAD = 0x8000;
	public static final int MAX_EXT_COUNT = 50;
	
	long previousExtensionOffset = -1;
	byte flags;
	int id;
	byte[] payload;
	
	long _selfOffset; //commodity for our parsing.
	
	static Extension ofSpec(int id, byte[] ba) {
		Extension ext = new Extension();
		ext.id = id;
		ext.payload = ba;
		ext.flags = FLAG_SPEC;
		return ext;
	}

	/**
	 * The flags are in an 'int' only for programming convenience; only the lowest 7 bits are used.
	 * Any attempt to set higher bits (0xFFFFFF80) in the flags by a class outside the package will fail.
	 * 
	 * @throws IllegalArgumentException if trying to set bits above 2^7.
	 */
	public static Extension ofCustom(int flags8bit, int id, byte[] ba) {
		if((flags8bit&0xffff_ff80) != 0)
			throw new IllegalArgumentException("trying to set flag bits");
		Extension ext = new Extension();
		ext.flags = (byte)(flags8bit & 0x7f);
		ext.id = id;
		ext.payload = ba;
		return ext;
	}
	
	/**
	 * Utility constructor to encode the payload as the UTF-8 bytes of the string.
	 * Equivalent to Extension.ofCustom(flags8bit, id, data.getBytes(StandardCharsets.UTF_8)).
	 */
	public static Extension ofCustom(int flags8bit, int id, String data) {
		return ofCustom(flags8bit, id, data.getBytes(StandardCharsets.UTF_8));
	}
	
	/**
	 * true if the FLAG_SPEC flag (2^7) is set.
	 */
	public boolean isSpec() {
		return (flags & FLAG_SPEC) != 0;
	}
	public byte getFlags() {
		return flags;
	}
	
	public int getId() {
		return id;
	}
	
	public byte[] getPayload() {
		return payload;
	}
	
	
	@Override
	public String toString() {
		return "Extension[" + (isSpec()?"SPEC " : "user ")
			+ ", flags="+Integer.toBinaryString(flags&0xff | 0x100).substring(1) 
			+ ", id="+id
			+ ", payload="
				+(payload!=null ? 
					new String(payload, 0, Math.min(50, payload.length), StandardCharsets.ISO_8859_1) 
					+ (payload.length>50 ? "...":"") + " ("+payload.length+" bytes)"
					: null)
			+"]";
	}

	
}