/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.Random;
import java.util.function.IntSupplier;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.github.ddeschenes_1.ragzip.TestUtils.DataGen;

class RagzipSanityTest {
	static final int PG_SZ_EXPONENT = 10; //keep PG_SZ_EXPONENT <= 24 for decent performance
	static final int IDX_SZ_EXPONENT = 5;
	static final int DATA_MB = 15;
	static final boolean USE_RANDOM_BYTES = true;
	
	static final int SAMPLING_QTY = 20_000;

	
	@Test
	void testOtherApis() throws IOException {
		String DATA = "hello ragzip world";
		byte[] payload = DATA.getBytes(StandardCharsets.ISO_8859_1);
		
		File gz = File.createTempFile("RagzipSanityTest_testRarerApis_", ".rgz", new File("."));
		gz.deleteOnExit();
		FileOutputStream fos = new FileOutputStream(gz);
		try (RagzipOutputStream zos = new RagzipOutputStream(fos, 10, 5)) {
			for(byte b: payload)
				zos.write(b&0xff); //testing the write(byte)
		}
		
		try(FileChannel fc = FileChannel.open(gz.toPath(), StandardOpenOption.READ) ) {
			try(RagzipFileChannel rag = new RagzipFileChannel(fc, 2, 2)) {
				ByteBuffer bb1 = ByteBuffer.allocate(6);
				ByteBuffer bb2 = ByteBuffer.allocate(7);
				ByteBuffer bb3 = ByteBuffer.allocate(5);
				
				//--- seek & scatter
				
				long n = rag.read(new ByteBuffer[] {bb1, bb2, bb3});
				assertEquals(DATA.length(), n);
				bb2.flip();
				assertEquals("ragzip ", new String(bb2.array(), bb2.arrayOffset(), bb2.limit()-bb2.position(), StandardCharsets.ISO_8859_1));
	
				//--- seek & read
				
				bb2.clear();
				n = rag.read(bb2, 3);
				assertEquals(bb2.capacity(), n);
				bb2.flip();
				assertEquals("lo ragz", new String(bb2.array(), bb2.arrayOffset(), bb2.limit()-bb2.position(), StandardCharsets.ISO_8859_1));
				
				
				File plain = File.createTempFile("RagzipSanityTest_testRarerApis_", ".txt", new File("."));
				plain.deleteOnExit();
				try(FileChannel plainCh = FileChannel.open(plain.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
					rag.transferTo(8L, 4L, plainCh);
				}
				
				assertEquals("gzip", Files.readString(plain.toPath(), StandardCharsets.ISO_8859_1));
				
			}
		}
	}

	@ParameterizedTest
	@CsvSource({
		"10, 5,       0, 0",
		"10, 5,    1000, 0",
		"10, 5,    1024, 0",
		"10, 5,    1025, 1",
		"10, 5,  0x8000, 1",
		"10, 5,  0x8001, 2",
		"10, 5,0x100000, 2", //10+5+5 = 20 bits
		"10, 5,0x100001, 3", //10+5+5 = 20 bits
	})
	void testMetadata(int pse, int ise, int size, int expectedLevels) throws IOException {
		File f = TestUtils.generateRagzip(USE_RANDOM_BYTES ? 20231210L : null, pse, ise, size, false);
		f.deleteOnExit();
		
		try(FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.READ) ) {
			try(RagzipFileChannel rag = new RagzipFileChannel(fc, 2, 2)) {
				assertEquals(0x00010000, rag.getVersion());
				
				int nlevels = 0;
				if(size>0) {
					long bits = (size-1) >>> pse;
					while (bits>0) {
						nlevels++;
						bits >>>= ise;
					}
				}
				
				assertEquals(0x00010000, rag.getVersion());
				assertEquals(expectedLevels, rag.getNumberOfLevels());
				assertEquals(nlevels, rag.getNumberOfLevels());
				assertEquals(ise, rag.getIndexSizeExponent());
				assertEquals(pse, rag.getPageSizeExponent());
				assertEquals(size, rag.size());
			}
		}
	}
	
	
	@ParameterizedTest
	@CsvSource({
		"true",
		"false",
	})
	void testSanityAtJumpPosition(boolean useChannelToWrite) throws IOException {
		Random random = USE_RANDOM_BYTES ? new Random(20231210) : null;
		final int size = DATA_MB<<20;
		System.out.println("\n\n========= test ragzip over "+(useChannelToWrite?"channel":"outputstream")+" output sanity ==============");
		File f = TestUtils.generateRagzip(USE_RANDOM_BYTES ? 20231210L : null, PG_SZ_EXPONENT, IDX_SZ_EXPONENT, size, useChannelToWrite);
		f.deleteOnExit();
		
		assertWholeDatasetSanityViaPlainGzip(f, new DataGen(random));
		
		long seed = System.nanoTime()& 0xffff_ffffL;
		System.out.println("\nUsing seed "+seed);
		
		Random location = new Random(seed);
		for(int i=0; i< 10; i++) {
			long loc = location.nextLong(size-SAMPLING_QTY);
			for(int cacheSizePerLevel: new int[] {-1, 0, 1}) {
				System.out.print("\nassert at "+loc+": "+", cacheSize="+cacheSizePerLevel);
				assertAtJumpLocation(f, new DataGen(random), loc, SAMPLING_QTY, cacheSizePerLevel);
			}
			System.out.println();
		}
	}
	
	
	static void assertWholeDatasetSanityViaPlainGzip(File f, IntSupplier dataGen) throws FileNotFoundException, IOException {
		try (FileInputStream fis = new FileInputStream(f);
			GZIPInputStream gzis = new GZIPInputStream(new BufferedInputStream(fis, 8192))) {
			
			int size = DATA_MB<<20;
			int pos = 0;
			byte[] exp = new byte[1<<16];
			byte[] act = new byte[exp.length];
			while(size>0) {
				System.out.print('.');
				int qty = Math.min(exp.length, size);
				for(int i=0; i<qty; i++)
					exp[i] = (byte)dataGen.getAsInt();
				gzis.readNBytes(act, 0, qty);
				int _pos = pos;
				assertArrayEquals(exp, act, () -> "bytes not equal in block at pos="+_pos);
				pos += qty;
				size -= qty;
			}
			int end = gzis.read();
			assertEquals(-1, end, "error at end != -1: " + end);
		}
	}
	
	
	static void assertAtJumpLocation(File f, DataGen expectedBytes, long logicalPos, int blockSize, int cacheSizePerLevel) throws IOException {
		for (int i = 0; i < logicalPos; i++)
			expectedBytes.getAsInt(); //skip to logical offset
		
		//System.out.println("\n\n----------------- ragzip channel reader sanity -------------------");
		int verbosity = 0;
		
		try (FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.READ);
			RagzipFileChannel rag = new RagzipFileChannel(fc, verbosity, cacheSizePerLevel)) {
			
			rag.position(logicalPos);
			ByteBuffer bb = ByteBuffer.allocate(blockSize);
			Utils.readFully(rag, bb, () -> "EOF");
			bb.flip();
			
			for (int i = 0; i < SAMPLING_QTY; i++) {
				int read = bb.get() & 0xff;
				int exp = expectedBytes.getAsInt();
				int _i = i;
				assertEquals(exp, read, () -> "Error at logicalPos " + logicalPos + "+" + _i + ": expected:" + exp + ", read=" + read);
				if (i % 10_000 == 0)
					System.out.print('.');
			}
		}
	}
	
	
	@ParameterizedTest
	@CsvSource({
		"0",
		"1024",
		"1025",
	})
	void testExtensionCountWriteLimit(int size) throws IOException {
		Random r = new Random(20241219);
		byte[] data1 = new byte[size];
		r.nextBytes(data1);
		
		byte[] bigext = new byte[Extension.MAX_EXT_PAYLOAD];
		r.nextBytes(bigext);
		
		File f = File.createTempFile("test.dat.ext-limit_", ".rgz", new File("."));
		f.deleteOnExit();
		try (FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
			fc.truncate(0);
			try (RagzipOutputStream zos = new RagzipOutputStream(fc, 10, 5)) {
				try {
					zos.appendExtension(Extension.ofCustom(0x1, Extension.MAX_EXT_COUNT+1, new byte[Extension.MAX_EXT_PAYLOAD+1]));
					fail("should have failed on excessively large extension over 32kB");
				} catch(IOException e) {
					//
				}
				
				zos.appendExtension(Extension.ofCustom(0x1, 1, bigext)); //test max payload
				
				for(int i=2; i<= Extension.MAX_EXT_COUNT; i++) {
					zos.appendExtension(Extension.ofCustom(0x1, i, "my extension "+i));
				}
				
				try {
					zos.appendExtension(Extension.ofCustom(0x1, Extension.MAX_EXT_COUNT+1, "my extension "+(Extension.MAX_EXT_COUNT+1)));
					fail("should have failed on extension #"+(Extension.MAX_EXT_COUNT+1));
				} catch(IOException e) {
					//
				}
				
				zos.write(data1);
			}
		}
		
		try (FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.READ)) {
			try (RagzipFileChannel rc = new RagzipFileChannel(fc)) {
				assertEquals(size>1024, rc.getNumberOfLevels()>0);
				
				LinkedList<Extension> extensions = rc.getExtensions();
				for(int i=1; i<= Extension.MAX_EXT_COUNT; i++) {
					Extension ext = extensions.get(i-1);
					assertEquals(i, ext.getId());
				}
				
				Extension ext = extensions.get(0); //1st is big ext
				byte[] payload = ext.getPayload();
				assertArrayEquals(bigext, payload);
			}
		}
	}
	
	@ParameterizedTest
	@CsvSource({
		"0",
		"1024",
		"1025",
	})
	void testExtensionCountReadLimit(int size) throws IOException {
		Random r = new Random(20241219);
		byte[] data1 = new byte[size];
		r.nextBytes(data1);
		
		File f = File.createTempFile("test.dat.ext-limit_", ".rgz", new File("."));
		f.deleteOnExit();
		try (FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
			fc.truncate(0);
			try (RagzipOutputStream zos = new RagzipOutputStream(fc, 10, 5)) {
				for(int i=1; i<=Extension.MAX_EXT_COUNT; i++) {
					zos.appendExtension(Extension.ofCustom(0x1, i, "my extension "+i));
				}
				
				//too big extension, which will not be written on finish:
				zos.extensions.add(Extension.ofCustom(0x1, Extension.MAX_EXT_COUNT+1, new byte[Extension.MAX_EXT_PAYLOAD+1]));
				
				//illicit extra extension will make its way to ragzip:
				zos.extensions.add(Extension.ofCustom(0x1, Extension.MAX_EXT_COUNT+2, "my extension "+(Extension.MAX_EXT_COUNT+2)));
				zos.write(data1);
			}
		}
		
		try (FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.READ)) {
			try (RagzipFileChannel rc = new RagzipFileChannel(fc)) {
				LinkedList<Extension> extensions = rc.getExtensions();
				assertEquals(Extension.MAX_EXT_COUNT, extensions.size());
				
				//1st extension (tail of linked list) will not be loaded because limit reached
				
				for(int i=2; i<=Extension.MAX_EXT_COUNT; i++) {
					Extension ext = extensions.get(i-2);
					assertEquals(i, ext.getId());
				}
				
				//reading last extension (th eillicit one)
				Extension ext = extensions.get(Extension.MAX_EXT_COUNT-1);
				assertEquals(Extension.MAX_EXT_COUNT+2, ext.getId());
			}
		}
	}
	
}
