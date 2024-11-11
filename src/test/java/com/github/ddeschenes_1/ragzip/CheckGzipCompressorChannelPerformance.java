/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPOutputStream;

public class CheckGzipCompressorChannelPerformance {
	
	public static void main(String[] args) throws IOException {
		System.out.println("""
			This program takes an input file and compresses it in many ragzip files
			with various parameterization which are revealed in the filename name.
			The tests are ran 3 times each (the 3rd one is usually the fastest).
			
			The filename will contain these tokens when it tests:
				-using nio and not (-nio and -stream)
				-default and no compression level (-Ld and -L0)
				-jdk.nio.enableFastFileTransfer true or false (-fast and -slow)
				-using FileChannel.transferTo or simple bytebuffer loop (-trfto and -bbloop)
			""");
		
		File f1 = new File(args.length>0 ? args[0] : "README.md");
		System.out.println("Each '.' is 1MB" + (f1 != null ? "; expecting " + (f1.length() >>> 20) + " MB" : ""));
		
		streamIt(args, -1);
		streamIt(args, -1);
		streamIt(args, -1);
		
		System.out.println("\n\n\n");

		streamIt(args, 0);
		streamIt(args, 0);
		streamIt(args, 0);
		
		channelIt(args, 0, false, false);
		channelIt(args, 0, false, false);
		channelIt(args, 0, false, false);
		
		channelIt(args, 0, false, true);
		channelIt(args, 0, false, true);
		channelIt(args, 0, false, true);
		
		channelIt(args, 0, true, false);
		channelIt(args, 0, true, false);
		channelIt(args, 0, true, false);
		
		channelIt(args, 0, true, true);
		channelIt(args, 0, true, true);
		channelIt(args, 0, true, true);
	}
	
	static void streamIt(String[] args, int complevel) throws IOException {
		_gzipIt(args, complevel, false, false, false);
	}
	
	static void channelIt(String[] args, int complevel, boolean useFFT, boolean useTrfTo) throws IOException {
		_gzipIt(args, complevel, true, useFFT, useTrfTo);
	}
	
	private static void _gzipIt(String[] args, int complevel, boolean useNio, boolean useFFT, boolean useTrfTo) throws IOException {
		if(useFFT)
			System.setProperty("jdk.nio.enableFastFileTransfer", "true");
		
		String inputFilename = args[0];
		String outputFilename = inputFilename
			+"-L"+(complevel<0?"d":complevel)
			+(useNio
				?"-nio" +(useFFT?"-fast":"-slow") +(useTrfTo?"-trfto":"-bbloop")
				:"-stream"
			);
		
		System.out.println("\n============ "+outputFilename+" ==================");
		
		File f1 = new File(inputFilename);
		if (!f1.isFile())
			throw new IllegalArgumentException("input file does not exist: " + f1);
		
		File f2 = new File(outputFilename+ ".gz");
		checkOutFileWritable(f2);
		
		if (useNio) {
			try (FileChannel ic = FileChannel.open(f1.toPath(), StandardOpenOption.READ)) {
				try (FileChannel oc = FileChannel.open(f2.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
					oc.truncate(0);
					try (GzipWritableChannel gzChannel = new GzipWritableChannel(oc, complevel)) {
						long t = System.currentTimeMillis();
						
						/////////////////////////////
						
						if(useTrfTo) {
							long pos = 0;
							long remaining = ic.size();
							while(remaining>0) {
								long len = ic.transferTo(pos, remaining, gzChannel);
								remaining -= len;
								pos += len;
							}
							
						} else {
							//ByteBuffer inBB = ByteBuffer.allocateDirect(8192);
							ByteBuffer inBB = ByteBuffer.allocate(8192);
							long lastDot = 0;
							long pos = 0;
							long end = ic.size();
							while(pos<end) {
								inBB.clear();
								int qty = ic.read(inBB);
								pos += qty;
								inBB.flip();
								while(inBB.hasRemaining())
									gzChannel.write(inBB);
								
								long currentDot = pos >>> 20;
								for (long d = lastDot + 1; d <= currentDot; d++) {
									System.out.print('.');
									if (d % 200 == 199)
										System.out.println();
								}
								lastDot = currentDot;
							}
						}
						
						gzChannel.finish();
						
						/////////////////////////////
						long d = Math.max(1,System.currentTimeMillis() - t);
						System.out.println("\nTook " + d + " ms, at ~ " + (ic.size() / d) + " kB/s into "+f2.length()+" bytes");
					}
				}
			}
		} else {
			try (InputStream is = f1 != null ? new FileInputStream(f1) : System.in) {
				try (OutputStream os = f2 != null ? new FileOutputStream(f2) : System.out) {
					long t = System.currentTimeMillis();
					OutputStream gzos = new GZIPOutputStream(os, 8192);
					
					try (gzos) {
						byte[] buff = new byte[1 << 20]; //1MB
						long size = 0;
						long lastDot = 0;
						int n;
						/////////////////////////////
						while ((n = is.read(buff)) >= 0) {
							size += n;
							gzos.write(buff, 0, n);
							long currentDot = size >>> 20;
							for (long d = lastDot + 1; d <= currentDot; d++) {
								System.out.print('.');
								if (d % 200 == 199)
									System.out.println();
							}
							lastDot = currentDot;
						}
						/////////////////////////////
					}
					
					long d = System.currentTimeMillis() - t;
					System.out.println("\nTook " + d + " ms, at ~ " + (f1.length() / d) + " kB/s into "+f2.length()+" bytes");
				}
			}
		}
	}
	
	private static void checkOutFileWritable(File f2) {
		File dir = f2.getParentFile();
		if (dir != null && !dir.exists() && !dir.mkdirs() && !dir.exists()) {
			throw new IllegalArgumentException("output file parent directory does not exist or cannot be created");
		}
	}
	
}
