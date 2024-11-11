/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class CheckRagzipJumpingPerformance {
	static final int PG_SZ_EXPONENT = 10;//keep <=16 for decent speed (minimal skip)
	static final int IDX_SZ_EXPONENT = 5;
	static final int DATA_MB = 15;
	static final int N_JUMPS = 20000;
	
	public static void main(String[] args) throws IOException {
		File f = new File(".", "test.dat-"+IDX_SZ_EXPONENT+"-"+PG_SZ_EXPONENT+".rgz");
		generateRagzip(f, IDX_SZ_EXPONENT, PG_SZ_EXPONENT, DATA_MB<<20);
		
		System.out.println("This program runs a series of "+N_JUMPS+ " random position jumps on a "+DATA_MB+" MB ragzip file.\n"+"""
			This is interactive so that you can hook a profiler like jvisualvm.
			Type 't' for short test (no page caching, no index cache, jump range on 100% of file),
			Type 'a' for full test,
			Type 'q' or 'x' to exit.
			Try many times for JIT compiler to kick in.
			""");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String line;
		LOOP: while((line=br.readLine())!=null) {
			switch(line) {
				case "a": testJumping(f,
						new boolean[] {false, true},
						new int[] {-1, 0, 1, 2, 4, 8, 16, 32},
						new double[] {1, 1, 1, 0.1, 0.01, 0.001, 0.0001}
				
					);
				break;
				
				case "t": testJumping(f,
					new boolean[] {false},
					new int[] {-1},
					new double[] {1, 1, 1}
					);
				break;
				
				case "q":
				case "x": break LOOP;
				default:
					System.out.println("t for short test, a for full test, q or x to exit");
			}
		}
		
	}
	
	public static File generateRagzip(File f, int idxSizeExponent, int pgSizeExponent, int size) throws IOException {
		Random r = new Random(20241218);
		try (FileOutputStream fos = new FileOutputStream(f)) {
			try (RagzipOutputStream zos = new RagzipOutputStream(fos, pgSizeExponent, idxSizeExponent)) {
				byte[] ba = new byte[1<<16];
				while(size>0) {
					r.nextBytes(ba);
					int n = Math.min(ba.length, size);
					zos.write(ba, 0, n);
					size -= n;
				}
			}
		}
		return f;
	}
	
	private static void testJumping(File f, boolean[] usePageCachingTests, int[] cacheSizeTests, double[] jumpRangesFraction) throws IOException {
		System.out.println("\n\n----------------- ragzip channel jumping -------------------");
		
		int verbosity = 0;
		int cachedPageSize = 1<<10; //1kB
		
		for(boolean usePageCaching: usePageCachingTests) {
		
			for(int cacheSize: cacheSizeTests) {
			
				try (FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.READ);
					RagzipFileChannel rag = new RagzipFileChannel(fc, verbosity, cacheSize)) {
					
					FileChannel topChannel = usePageCaching ? new CachingSeekableReadByteChannel(cachedPageSize, 3, rag) : rag;
					
					Random seekRnd = new Random(20241118);
					ByteBuffer bb = ByteBuffer.allocate(64); //read a small block
					
					
					for(double jumpRangePercent: jumpRangesFraction) {
						long jumpRange = (long)(jumpRangePercent*rag.size());
						long t = System.currentTimeMillis();
						for(int i=0; i< N_JUMPS; i++) {
							long pos = seekRnd.nextLong(jumpRange - bb.capacity());
							topChannel.position(pos);
							Utils.readFully(topChannel, bb, () -> "EOF");
							bb.clear();
						}
						long d = System.currentTimeMillis() - t;
						System.out.println("Index cache size of "+cacheSize
							+ (usePageCaching? ", USING PAGE CACHING" : ", no page cache")
							+", took "+d+" ms, at ~ "+(1e3*d/N_JUMPS)+" us/jump"
							+ " (jump range +/-"+jumpRange+")"
						);
					}
					System.out.println();
				}
			}
		}
	}
	
}
