package com.github.ddeschenes_1.ragzip;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ExtensionTest {
	
	@Test
	void testOfCustomIntIntByteArray() {
		for(int i=0x8000_0000; i>=0x80; i>>>=1) {
			int f = i;
			assertThrows(IllegalArgumentException.class, () -> Extension.ofCustom(f, f, "my ext"));
		}
		
		for(int i=0x40; i>0; i>>>=1) {
			int f = i;
			Extension ext = Extension.ofCustom(f, 1000+f, "my ext");
			assertEquals(f, ext.getFlags());
			assertEquals(1000+f, ext.getId());
		}
		
		Extension.ofCustom(0, 0, "my ext");
	}
	
	
	
}
