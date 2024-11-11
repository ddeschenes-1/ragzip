package com.github.ddeschenes_1.ragzip;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RagzipParallelEncoderDecoderTest {
	
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
	void testEncodeDecode(int pse, int ise, int size) throws IOException, InterruptedException {
		File f = File.createTempFile("test.dat-par_i"+ise+"-p"+pse+"-L"+size+"_", ".dat", new File("."));
		File f2 = new File(f.getParentFile(), f.getName()+".rgz");
		File f3 = new File(f.getParentFile(), f.getName()+".decoded");
		f.deleteOnExit();
		f2.deleteOnExit();
		f3.deleteOnExit();
		
		byte[] data = new byte[size];
		Random r = new Random(System.nanoTime());
		r.nextBytes(data);
		Files.write(f.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		
		RagzipParallelEncoder pe = new RagzipParallelEncoder(f, f2, pse, ise).withVerbosity(0);
		pe.startEncoding();
		
		try (FileChannel fc = FileChannel.open(f2.toPath(), StandardOpenOption.READ)) {
			try (RagzipFileChannel rc = new RagzipFileChannel(fc)) {
				byte[] data2 = new byte[size];
				ByteBuffer bb = ByteBuffer.wrap(data2);
				Utils.readFully(rc, bb, () -> "EOF");
				bb.flip();
				assertArrayEquals(data, data2);
			}
		}
		
		RagzipParallelDecoder de = new RagzipParallelDecoder(f2, f3, 0);
		de.startDecoding();
		
		byte[] data2 = Files.readAllBytes(f3.toPath());
		assertArrayEquals(data, data2);
	}
	
}
