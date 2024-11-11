package com.github.ddeschenes_1.ragzip;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class GzipRWChannelTest {
	static final int DATASIZE = 100000;
	static final byte[] DATA = new byte[DATASIZE];
	static byte[] gz;
	
	@BeforeAll
	public static void setupMyClass() throws IOException {
		Random r = new Random(20241213);
		r.nextBytes(DATA);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gzos = new GZIPOutputStream(baos);
		gzos.write(DATA);
		gzos.finish();
		gzos.flush();
		gzos.close();
		gz = baos.toByteArray();
	}
	
	@Test
	void testWrite() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		WritableByteChannel wc = Channels.newChannel(baos);
		GzipWritableChannel gzch = new GzipWritableChannel(wc);
		
		ByteBuffer bb = ByteBuffer.wrap(DATA);
		Utils.writeFully(bb, gzch);
		gzch.finish();
		gzch.close();
		byte[] gzres = baos.toByteArray();
		//output could have been slightly different yet store same data.
		//it is just nice that it can be exactly equal:
		assertArrayEquals(gz, gzres);
		
		GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(gzres)); 
		byte[] ba2 = gzis.readAllBytes();
		assertArrayEquals(DATA, ba2);
	}
	
	@Test
	void testRead() throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(gz);
		ReadableByteChannel rc = Channels.newChannel(bais);
		GzipReadableChannel gzch = new GzipReadableChannel(rc);
		
		byte[] res = new byte[DATASIZE];
		ByteBuffer bb = ByteBuffer.wrap(res);
		Utils.readFully(gzch, bb, () -> "EOF");
		assertEquals(-1, gzch.read(ByteBuffer.wrap(new byte[1])));
		gzch.close();
		assertArrayEquals(DATA, res);
	}
	
	@Test
	void testReadMultiMember() throws IOException {
		ByteArrayInputStream bais1 = new ByteArrayInputStream(gz);
		ByteArrayInputStream bais2 = new ByteArrayInputStream(gz);
		ReadableByteChannel rc = Channels.newChannel(new SequenceInputStream(bais1, bais2));
		GzipReadableChannel gzch = new GzipReadableChannel(rc);
		
		byte[] res = new byte[2*DATASIZE];
		ByteBuffer bb = ByteBuffer.wrap(res);
		Utils.readFully(gzch, bb, () -> "EOF");
		assertEquals(-1, gzch.read(ByteBuffer.wrap(new byte[1])));
		gzch.close();
		
		byte[] ba2 = new byte[2*DATASIZE];
		System.arraycopy(DATA, 0, ba2, 0, DATA.length);
		System.arraycopy(DATA, 0, ba2, DATA.length, DATA.length);
		assertArrayEquals(ba2, res);
	}
	
	@Test
	void testRead1stOfMultiMember() throws IOException {
		ByteArrayInputStream bais1 = new ByteArrayInputStream(gz);
		ByteArrayInputStream bais2 = new ByteArrayInputStream(gz);
		ReadableByteChannel rc = Channels.newChannel(new SequenceInputStream(bais1, bais2));
		GzipReadableChannel gzch = new GzipReadableChannel(rc, false);
		
		byte[] res = new byte[DATASIZE];
		ByteBuffer bb = ByteBuffer.wrap(res);
		Utils.readFully(gzch, bb, () -> "EOF");
		assertEquals(-1, gzch.read(ByteBuffer.wrap(new byte[1])));
		gzch.close();
		assertArrayEquals(DATA, res);
	}
	
	
	@ParameterizedTest
	@CsvSource({
		"0, 0",
		"0, 99",
		"0, 999",
		"0, 9999",
		"0, 79999",
		"9, 0",
		"9, 99",
		"9, 999",
		"9, 9999",
		"9, 79999",
		"99, 0",
		"99, 99",
		"99, 999",
		"99, 9999",
		"99, 79999",
		"999, 0",
		"999, 99",
		"999, 999",
		"999, 9999",
		"999, 79999",
		"9999, 0",
		"9999, 99",
		"9999, 999",
		"9999, 9999",
		"9999, 79999",
	})
	void testSkipNBytes(int read, int distance) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(gz);
		ReadableByteChannel rc = Channels.newChannel(bais);
		GzipReadableChannel gzch = new GzipReadableChannel(rc);
		
		byte[] res = new byte[DATASIZE];
		ByteBuffer bb = ByteBuffer.wrap(res);
		
		ByteBuffer sl = bb.slice(0, read);
		Utils.readFully(gzch, sl, () -> "EOF");
		sl.flip();
		assertEquals(read, sl.limit());
		
		gzch.skipNBytes(distance);
		
		bb.position(read+distance);
		Utils.readFully(gzch, bb, () -> "EOF");
		bb.flip();
		assertEquals(DATASIZE, bb.limit());
		
		assertEquals(-1, gzch.read(ByteBuffer.wrap(new byte[1])));
		gzch.close();
		
		for(int i=0; i<read; i++) {
			int _i = i;
			assertEquals(DATA[i], res[i], () -> "byte ["+_i+"] differ");
		}
		for(int i=read; i<read+distance; i++) {
			int _i = i;
			assertEquals(0, res[i], () -> "byte ["+_i+"] differ");
		}
		for(int i=read+distance; i<DATA.length; i++) {
			int _i = i;
			assertEquals(DATA[i], res[i], () -> "byte ["+_i+"] differ");
		}
	}
	

	@ParameterizedTest
	@CsvSource({
		"/allmeta-no-hcrc.gz",
		"/allmeta-hcrc.gz"
	})
	void testReadAllMeta(String resourceName) throws IOException {
		InputStream is = this.getClass().getResourceAsStream(resourceName);
		ReadableByteChannel rc = Channels.newChannel(is);
		GzipReadableChannel gzch = new GzipReadableChannel(rc);
		
		byte[] data = new byte[1000];
		Random r = new Random(20241213);
		r.nextBytes(data);
		
		byte[] res = new byte[1000];
		ByteBuffer bb = ByteBuffer.wrap(res);
		Utils.readFully(gzch, bb, () -> "EOF");
		assertArrayEquals(data, res);
		
		res = new byte[1000];
		bb = ByteBuffer.wrap(res);
		Utils.readFully(gzch, bb, () -> "EOF");
		assertArrayEquals(data, res);
		
		assertEquals(-1, gzch.read(ByteBuffer.wrap(new byte[1])));
		gzch.close();
	}
	
	
	
}
