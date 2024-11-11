/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CachingSeekableReadByteChannelTest {
	static final int PAGE_SIZE = 100;
	static final int MAX_PAGES = 3;
	
	File f;
	FileChannel fc;
	CachingSeekableReadByteChannel cc;
	
	@BeforeEach
	void setUp() throws Exception {
		//f = new File("test.dat.caching");
		f = File.createTempFile("test.dat."+this.getClass().getSimpleName()+"_", ".dat", new File("."));
		f.deleteOnExit();
		try (FileOutputStream fos = new FileOutputStream(f);
			BufferedOutputStream bos = new BufferedOutputStream(fos)) {
			for(int i=0; i< 10*PAGE_SIZE; i++) {
				bos.write(i); //0..255
			}
			bos.flush();
		}
		
		fc = FileChannel.open(f.toPath(), StandardOpenOption.READ);
		cc = new CachingSeekableReadByteChannel(PAGE_SIZE, MAX_PAGES, fc);
	}
	
	@AfterEach
	void tearDown() throws Exception {
		cc.close();
		fc.close();
	}
	
	
	static byte[] toArray(ByteBuffer bb) {
		int markbak = -1;
		int posbak = bb.position();
		//int limitbak = bb.limit();
		try {
			bb.reset();
			markbak = bb.position();
		} catch(InvalidMarkException e) {//
		}
		if(markbak>=0)
			bb.position(markbak);
		bb.mark();
		bb.position(posbak);
		
		//---
		
		byte[] ba = new byte[bb.remaining()];
		bb.get(ba);
		bb.position(posbak);
		return ba;
	}
	
	static String toString(ByteBuffer bb) {
		return new String(toArray(bb), StandardCharsets.ISO_8859_1);
	}
	
	
	static byte[] bytesOf(String s) {
		return s.getBytes(StandardCharsets.ISO_8859_1);
	}
	
	static byte[] bytesOf(int... ia) {
		byte[] ba = new byte[ia.length];
		for(int i=0; i< ia.length; i++)
			ba[i] = (byte)ia[i];
		return ba;
	}
	
	
	//=====================
	
	@Test
	void testHitMissRolling() throws IOException {
		//read at specific positions
		_testReadAt('2', 5, bytesOf("23456"), false); //cache miss, all in cache
		_testReadAt('A', 5, bytesOf("ABCDE"), true); //cache hit,  all in cache
		_testReadAt(95, 10, bytesOf(95, 96, 97, 98, 99), true);//premature end at page end.
		_testReadAt(95, 10, bytesOf(95, 96, 97, 98, 99), true);//premature end at page end, repeat should not change anything
		
		
		_testReadAt(350, 3, bytesOf(350, 351, 352), false); //2nd page
		_testReadAt(950, 3, bytesOf(950, 951, 952), false); //3rd page
		_testReadAt('A', 5, bytesOf("ABCDE"), true); //1st page still cached
		_testReadAt(650, 3, bytesOf(650, 651, 652), false); //4th page, LRU page falling off is 350
		
		assertFalse(cc.hasPage(150), "page 1 should not exist");
		assertFalse(cc.hasPage(250), "page 2 should not exist");
		assertFalse(cc.hasPage(450), "page 4 should not exist");
		assertFalse(cc.hasPage(550), "page 5 should not exist");
		assertFalse(cc.hasPage(750), "page 7 should not exist");
		assertFalse(cc.hasPage(850), "page 8 should not exist");
	}
	
	void _testReadAt(int pos, int buflen, byte[] expected, boolean cachehit) throws IOException {
		assertEquals(cachehit, cc.hasPage(pos));

		ByteBuffer bb = ByteBuffer.allocate(buflen);
		
		cc.position(pos);
		int n = cc.read(bb);
		assertEquals(pos+expected.length, cc.position());
		assertTrue(cc.hasPage(pos));
		
		byte[] ba = new byte[n];
		bb.flip();
		bb.get(ba);
		assertArrayEquals(expected, ba);
	}
	
	@Test
	void testScatter() throws IOException {
		//--- in page-----
		
		cc.position('A');
		ByteBuffer bb1 = ByteBuffer.allocate(4);
		ByteBuffer bb2 = ByteBuffer.allocate(3);
		ByteBuffer bb3 = ByteBuffer.allocate(0);
		ByteBuffer bb4 = ByteBuffer.allocate(5);
		ByteBuffer[] bba = {bb1, bb2, bb3, bb4};
		long n = cc.read(bba);
		assertEquals(12, n);
		
		for(ByteBuffer bb: bba)
			bb.flip();
		assertEquals("ABCD", toString(bb1));
		assertEquals("EFG", toString(bb2));
		assertEquals("", toString(bb3));
		assertEquals("HIJKL", toString(bb4));
		
		//--- meet page end -----
		
		cc.position(90);
		for(ByteBuffer bb: bba)
			bb.clear();
		n = cc.read(new ByteBuffer[] {bb1, bb2, bb3, bb4});
		assertEquals(10, n);
		for(ByteBuffer bb: bba)
			bb.flip();
		
		assertArrayEquals(bytesOf(90, 91, 92, 93), toArray(bb1));
		assertArrayEquals(bytesOf(94, 95, 96), toArray(bb2));
		assertArrayEquals(bytesOf(), toArray(bb3));
		assertArrayEquals(bytesOf(97, 98, 99), toArray(bb4)); //imcomplete due to page ended
	}
	
	@Test
	void testTransferTo() throws IOException {
		//--- in page-----
		
		WritableByteChannel dst = new WritableByteChannel() {
			@Override
			public boolean isOpen() {return true;}
			@Override
			public void close() throws IOException {/**/}
			@Override
			public int write(ByteBuffer src) throws IOException {
				int n = src.remaining();
				src.position(src.limit());
				return n;
			}
		};
		
		cc.position('2');
		assertFalse(cc.hasPage('2'));
		assertFalse(cc.hasPage('A'));
		
		long n = cc.transferTo('A', 10, dst); //in page
		assertEquals(10, n);
		assertEquals('2', cc.position());
		assertTrue(cc.hasPage('A'));
		assertFalse(cc.hasPage(100));
		
		n = cc.transferTo(95, 10, dst); //fetches next page
		assertEquals(10, n);
		assertEquals('2', cc.position());
		assertTrue(cc.hasPage(95));
		assertTrue(cc.hasPage(100));
		
		n = cc.transferTo(95, 700, dst); //fetches many pages 0..7, rolls off all but last 3
		assertEquals(700, n);
		assertEquals('2', cc.position());
		for(int i=0; i<=11; i++)
			assertEquals(i>=5 && i<=7, cc.hasPage(i*100));
	}
	
}

