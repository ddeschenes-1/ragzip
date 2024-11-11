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

import com.github.ddeschenes_1.ragzip.RagzipFileChannel;
import com.github.ddeschenes_1.ragzip.RagzipParallelDecoder;

public class RagzipParallelDecoderMain {
	static void help(PrintStream ps) {
		ps.println("""
			Decompress a ragzip file, using multiple threads.
			Options:
			   -i <filename> //input file to decompress (must have .gz or .rgz extension)
			   -o <filename> //output file (default: input filename minus extension in same directory)
			   -s            //show specifications only, do not decompress
			   -v            //verbose output
			   -vv           //very verbose output
			   --clobber     //replace output file if exists
			   
			   -h            //this help
			""");
		
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		String filename = null;
		String outfilename = null;
		boolean clobber = false;
		boolean specsOnly = false;
		int verbosity = 0;
		
		int i=0;
		while(i<args.length) {
			String opt = args[i++];
			switch(opt) {
				case "-i": filename = args[i++]; break;
				case "-o": outfilename = args[i++]; break;
				case "--clobber": clobber = true; break;
				case "-s": specsOnly = true; break;
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
		if(!filename.toLowerCase().endsWith(".gz") && !filename.toLowerCase().endsWith(".rgz"))
			throw new IllegalArgumentException("file is not .gz or .rgz");
		
		File f1 = new File(filename);
		if(!f1.isFile())
			throw new IllegalArgumentException("input file does not exist: "+f1);
		
		if(specsOnly) {
			showSpecs(f1);
			System.exit(0);
		}
		
		File f2 = outfilename!=null ? new File(outfilename) : new File(f1.getParent(), f1.getName().substring(0, f1.getName().lastIndexOf('.')));
		File dir = f2.getParentFile();
		if(dir!=null && !dir.exists() && !dir.mkdirs() && !dir.exists()) {
			throw new IllegalArgumentException("output file parent directory does not exist or cannot be created");
		}
		
		if(f2.exists() && !clobber)
			throw new IllegalArgumentException("output file exist, use --clobber to overwrite");
		
		if(verbosity>0) {
			log("ragzip decoding "+f1+" to "+f2);
		}
		
		RagzipParallelDecoder rpe = new RagzipParallelDecoder(f1, f2, verbosity);
		try (FileChannel fc2 = FileChannel.open(f2.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
			fc2.truncate(0);
		}
		
		long t = System.currentTimeMillis();
		rpe.startDecoding();
		long d = System.currentTimeMillis() - t;
		if(verbosity>0)
			log("\nTook "+d+" ms, at ~ "+(f2.length()/d)+" kB/s");
		
		if(verbosity>0)
			log("Done ragzip decoding "+f1+" to "+f2);
	}
	
	
	static void showSpecs(File f) throws IOException {
		try (FileChannel fc = FileChannel.open(f.toPath(),StandardOpenOption.READ);
			RagzipFileChannel rc = new RagzipFileChannel(fc, 1, 0); //does some printout in verbose==1
			) {
			//all been said.
		}
	}
}