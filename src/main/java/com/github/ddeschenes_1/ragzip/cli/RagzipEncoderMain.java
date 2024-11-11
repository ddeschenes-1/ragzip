/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import com.github.ddeschenes_1.ragzip.RagzipOutputStream;

/**
 * CLI tool to compress a file into the ragzip format, single-threaded.
 * 
 */
public class RagzipEncoderMain {
	
	static void help(PrintStream ps) {
		ps.println("""
			Compress a file to ragzip format (single threaded).
			Options:
			   -P <n>        //page size exponent for power of 2 (default: 13)
			   -I <n>        //index size exponent for power of 2 (default: 12)
			   
			   -i <filename> //input file to compress
			   -o <filename> //output file (default: input filename + '.gz' in same directory)
			   -v            //verbose output
			   --clobber     //replace output file if exists
			   
			   -h            //this help
			""");
	}
	
	public static void main(String[] args) throws IOException {
		int pgExp = 13;
		int idxExp = 12;
		String inputFilename = null;
		String outputFilename = null;
		boolean clobber = false;
		boolean verbose = false;
		
		int i=0;
		while(i<args.length) {
			String opt = args[i++];
			switch(opt) {
				case "-P": pgExp = Integer.parseInt(args[i++]); break;
				case "-I": idxExp = Integer.parseInt(args[i++]); break;
				
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
			f1 = new File(inputFilename);
			if(!f1.isFile())
				throw new IllegalArgumentException("input file does not exist: "+f1);
			
			if(outputFilename==null || !"-".equals(outputFilename)) {
				f2 = outputFilename!=null ? new File(outputFilename) : new File(f1.getParent(), f1.getName()+".gz");
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
		
		verbose &= (f2!=null);
		
		if(verbose)
			System.out.println("Using page size 2^"+pgExp+", index size 2^"+idxExp);
		
		try (InputStream is = f1!=null ? new FileInputStream(f1) : System.in;
			OutputStream os = f2!=null ? new FileOutputStream(f2) : System.out;
			RagzipOutputStream ragzipos = new RagzipOutputStream(os, pgExp, idxExp)
			){
			
			long t = System.currentTimeMillis();
			
			if(verbose)
				System.out.println("Each '.' is 1MB"+ (f1!=null ? ", expecting "+(f1.length()>>>20)+" MB" : ""));
			
			byte[] buff = new byte[1<<20];
			long size = 0;
			long lastDot = 0;
			int n;
			while((n=is.read(buff)) > 0) {
				size += n;
				ragzipos.write(buff, 0, n);
				
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
		
		if(verbose)
			System.out.println("Done ragzip encoding "+(f1==null?"stdin":""+f1)+" to "+f2);
		
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
