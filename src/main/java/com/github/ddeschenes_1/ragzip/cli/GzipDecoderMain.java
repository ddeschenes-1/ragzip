/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip.cli;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;

import com.github.ddeschenes_1.ragzip.GzipReadableChannel;

/**
 * Simple CLI tool wrapping the GzipReadableChannel and decoding with a single thread.
 * 
 * Provided just as pure-java partial alternative to gunzip.
 */
public class GzipDecoderMain {
	
	static void help(PrintStream ps) {
		ps.println("""
			Decompress a gzip file or stdin (single threaded).
			Options:
			   -i <filename> //input file to decompress (must have .gz or .rgz extension)
			                 //    will use stdin if no input file given.
			   -o <filename> //output file (default: input filename minus extension in same directory,
			                 //    or stdout when stdin is used; use '-' for stdout even with an inputfile)
			   -v            //verbose output
			   --clobber     //replace output file if exists (ignored when using stdout)
			   
			   -h            //this help
			""");
	}
	
	
	public static void main(String[] args) throws IOException {
		String inputFilename = null;
		String outputFilename = null;
		boolean clobber = false;
		boolean verbose = false;
		
		int i=0;
		while(i<args.length) {
			String opt = args[i++];
			switch(opt) {
				case "-i": inputFilename = args[i++]; break;
				case "-o": outputFilename = args[i++]; break;
				case "--clobber": clobber = true; break;
				
				case "-v": verbose = true; break;
				case "-h": help(System.out); System.exit(0); break;
				
				default:
					System.err.println("unknown option "+opt);
					help(System.err);
					System.exit(1);
			}
		}
		
		File f1 = null;
		File f2 = null;
		
		if(inputFilename!=null) {
			if(!inputFilename.toLowerCase().endsWith(".gz") && !inputFilename.toLowerCase().endsWith(".rgz"))
				throw new IllegalArgumentException("file is not .gz or .rgz");
			
			f1 = new File(inputFilename);
			if(!f1.isFile())
				throw new IllegalArgumentException("input file does not exist: "+f1);
			
			if(outputFilename==null || !"-".equals(outputFilename)) {
				f2 = outputFilename!=null ? new File(outputFilename) : new File(f1.getParent(), f1.getName().substring(0, f1.getName().lastIndexOf('.')));
				checkOutFileWritable(f2, clobber);
				if(verbose)
					System.out.println("ragzip encoding "+f1+" to "+f2);
			} else {
				//use stdout
			}
		} else {
			if(outputFilename!=null && !"-".equals(outputFilename)) {
				f2 = new File(outputFilename);
				checkOutFileWritable(f2, clobber);
				if(verbose)
					System.out.println("ragzip encoding "+f1+" to "+f2);
			} else {
				//use stdout
			}
		}
		
		if(verbose)
			System.out.println("gzip decoding "+f1+" to "+f2);
		
		try (ReadableByteChannel rc = f1!=null ? FileChannel.open(f1.toPath(), StandardOpenOption.READ) : Channels.newChannel(System.in)) {
			try (WritableByteChannel wc = f2!=null ? FileChannel.open(f2.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE) : Channels.newChannel(System.out)) {
				try (GzipReadableChannel gzrc = new GzipReadableChannel(rc)) {
					long t = System.currentTimeMillis();
					if(verbose)
						System.out.println("Each '.' is 1MB"+(f1!=null?"; expecting "+(f1.length()>>>20)+" MB" : ""));
					
					ByteBuffer bb = ByteBuffer.allocateDirect(1<<20);
					
					long size = 0;
					long lastDot = 0;
					int n;
					while((n=gzrc.read(bb.clear())) > 0) {
						size += n;
						wc.write(bb.flip());
						long currentDot = size>>>20;
						for(long d=lastDot+1; d<=currentDot; d++) {
							System.out.print('.');
							if (d % 100 == 99)
								System.out.println();
						}
						lastDot = currentDot;
					}
					
					long d = System.currentTimeMillis() - t;
					if(verbose)
						System.out.println("\nTook "+d+" ms, at ~ "+(size/d)+" kB/s");
				}
			}
		}
		
		if(verbose)
			System.out.println("Done gzip decoding "+(f1==null?"stdin":""+f1)+" to "+f2);
		
	}
	
	private static void checkOutFileWritable(File f2, boolean clobber) {
		File dir = f2.getParentFile();
		if(dir!=null && !dir.exists() && !dir.mkdirs() && !dir.exists()) {
			throw new IllegalArgumentException("output file parent directory does not exist or cannot be created");
		}
		
		if(f2.exists() && !clobber)
			throw new IllegalArgumentException("output file exist, use --clobber to overwrite");
	}
	
}
