/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip.cli;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import com.github.ddeschenes_1.ragzip.GzipWritableChannel;

/**
 * Simple CLI tool wrapping the GzipWritableChannel and decoding with a single thread.
 * 
 * Provided just as pure-java partial alternative to gzip.
 */
public class GzipEncoderMain {
	
	static void help(PrintStream ps) {
		ps.println("""
			Compress a file to gzip format (single threaded).
			Options:
			   -i <filename> //input file to compress
			   -o <filename> //output file (default: input filename + '.gz' in same directory)
			   -v            //verbose output
			   --clobber     //replace output file if exists
			   
			   -h            //this help
			""");
	}
	
	public static void main(String[] args) throws IOException {
		String inputFilename = null;
		String outputFilename = null;
		boolean clobber = false;
		boolean verbose = false;
		
		int i = 0;
		while (i < args.length) {
			String opt = args[i++];
			//@formatter:off
			switch (opt) {
				case "-i":inputFilename = args[i++];break;
				case "-o":outputFilename = args[i++];break;
				case "--clobber":clobber = true;break;
				
				case "-v":verbose = true;break;
				case "-h":
					help(System.out);
					System.exit(0);
				break;
			
				default:
					System.err.println("unknown option " + opt);
					help(System.err);
					System.exit(1);
			}
			//@formatter:on
		}
		
		File f1 = null;
		File f2 = null;
		
		if (inputFilename != null) {
			f1 = new File(inputFilename);
			if (!f1.isFile())
				throw new IllegalArgumentException("input file does not exist: " + f1);
			
			f2 = outputFilename != null ? new File(outputFilename) : new File(f1.getParent(), f1.getName() + ".gz");
			checkOutFileWritable(f2, clobber);
			if (verbose)
				System.out.println("ragzip encoding " + f1 + " to " + f2);
		} else {
			throw new IllegalArgumentException("missing input file (stdin not supported)");
		}
		
		if (verbose)
			System.out.println("gzip encoding " + f1 + " to " + f2);
		
		try (FileChannel ic = FileChannel.open(f1.toPath(), StandardOpenOption.READ)) {
			try (FileChannel oc = FileChannel.open(f2.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
				oc.truncate(0);
				try (GzipWritableChannel gzChannel = new GzipWritableChannel(oc)) {
					long t = System.currentTimeMillis();
					
					if (verbose)
						System.out.println("Each '.' is 1MB" + (f1 != null ? "; expecting " + (f1.length() >>> 20) + " MB" : ""));
					
					ByteBuffer bb = ByteBuffer.allocateDirect(8192);
					long lastDot = 0;
					long pos = 0;
					long end = ic.size();
					while(pos<end) {
						bb.clear();
						int qty = ic.read(bb);
						if(qty<0)
							throw new EOFException("premature end of file at pos = "+pos);
						if(qty==0)
							continue;
						
						pos += qty;
						bb.flip();
						while(bb.hasRemaining())
							gzChannel.write(bb);
						
						long currentDot = pos >>> 20;
						for (long d = lastDot + 1; d <= currentDot; d++) {
							System.out.print('.');
							if (d % 100 == 99)
								System.out.println();
						}
						lastDot = currentDot;
					}
					
					gzChannel.finish();
					
					/////////////////////////////
					long d = Math.max(1,System.currentTimeMillis() - t);
					if (verbose)
						System.out.println("\nTook " + d + " ms, at ~ " + (ic.size() / d) + " kB/s");
				}
			}
		}
		
		if (verbose)
			System.out.println("Done gzip decoding " + (f1 == null ? "stdin" : "" + f1) + " to " + f2);
		
	}
	
	private static void checkOutFileWritable(File f2, boolean clobber) {
		File dir = f2.getParentFile();
		if (dir != null && !dir.exists() && !dir.mkdirs() && !dir.exists()) {
			throw new IllegalArgumentException("output file parent directory does not exist or cannot be created");
		}
		
		if (f2.exists() && !clobber)
			throw new IllegalArgumentException("output file exist, use --clobber to overwrite");
	}
	
}
