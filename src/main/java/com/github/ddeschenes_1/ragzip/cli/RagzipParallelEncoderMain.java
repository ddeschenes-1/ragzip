/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip.cli;

import static com.github.ddeschenes_1.ragzip.Utils.log;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import com.github.ddeschenes_1.ragzip.RagzipParallelEncoder;

public class RagzipParallelEncoderMain {
	
	static void help(PrintStream ps) {
		ps.println("""
			Compress a file to ragzip format, using multiple threads.
			Options:
			   -P <n>        //page size exponent for power of 2 (default: 13)
			   -I <n>        //index size exponent for power of 2 (default: 12)
			   
			   -i <filename> //input file to compress
			   -o <filename> //output file (default: input filename + '.gz' in same directory)
			   -v            //verbose output
			   -vv           //very verbose output
			   --clobber     //replace output file if exists
			   
			   -h            //this help
			""");
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		int pgExp = 13;
		int idxExp = 12;
		String filename = null;
		String outfilename = null;
		boolean clobber = false;
		int verbosity = 0;
		
		int i=0;
		while(i<args.length) {
			String opt = args[i++];
			switch(opt) {
				case "-P": pgExp = Integer.parseInt(args[i++]); break;
				case "-I": idxExp = Integer.parseInt(args[i++]); break;
				
				case "-i": filename = args[i++]; break;
				case "-o": outfilename = args[i++]; break;
				case "--clobber": clobber = true; break;
				
				case "-v": verbosity = 1; break;
				case "-vv": verbosity = 2; break;
				case "-h": help(System.out); System.exit(0); break;
				
				default:
					System.err.println("unknown option "+opt);
					help(System.err);
					System.exit(1);
			}
		}
		
		if(filename==null)
			throw new IllegalArgumentException("missing -i option");
		
		File f1 = new File(filename);
		if(!f1.isFile())
			throw new IllegalArgumentException("input file does not exist: "+f1);
		
		File f2 = outfilename!=null ? new File(outfilename) : new File(f1.getParent(), f1.getName()+".gz");
		File dir = f2.getParentFile();
		if(dir!=null && !dir.exists() && !dir.mkdirs() && !dir.exists()) {
			throw new IllegalArgumentException("output file parent directory does not exist or cannot be created");
		}
		
		if(f2.exists() && !clobber)
			throw new IllegalArgumentException("output file exist, use --clobber to overwrite");
		
		if(verbosity>0) {
			log("ragzip encoding "+f1+" to "+f2);
			log("Using page size 2^"+pgExp+", index size 2^"+idxExp);
		}
		
		RagzipParallelEncoder rpe = new RagzipParallelEncoder(f1, f2, pgExp, idxExp);
		rpe.withVerbosity(verbosity);
		
		try (FileChannel fc2 = FileChannel.open(f2.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
			fc2.truncate(0);
		}
		
		long t = System.currentTimeMillis();
		rpe.startEncoding();
		long d = System.currentTimeMillis() - t;
		if(verbosity>0)
			log("\nTook "+d+" ms, at ~ "+(f1.length()/d)+" kB/s");
		
		if(verbosity>0)
			log("Done ragzip encoding "+f1+" to "+f2);
		
		
	}
	
}