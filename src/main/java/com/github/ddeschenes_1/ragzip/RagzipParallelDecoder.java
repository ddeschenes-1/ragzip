/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import static com.github.ddeschenes_1.ragzip.Utils.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import com.github.ddeschenes_1.ragzip.ExtraField.SubField;
import com.github.ddeschenes_1.ragzip.Utils.BBIS;
import com.github.ddeschenes_1.ragzip.Utils.ClosingTF;
import com.github.ddeschenes_1.ragzip.Utils.InterruptableAction;
import com.github.ddeschenes_1.ragzip.Utils.TPool;

/**
 * Decompress a ragzip input file, using a pipeline of multi threaded stages.
 * There a 4 stages: tree walking, reading, unzipping, writing.
 * There are 3 queues: rQ, zQ and wQ which are reported by the monitor every seconds, along with progress. 
 * <p>
 * This tool does not read stdin since it requires direct input file access to slice it in pages.
 * <p>
 * The maximum page size exponents supported is 21 (2^21 = 2MB) because pages are loaded in memory. 
 */
public class RagzipParallelDecoder {
	int verbosity = 0;
	
	final int pageSizeExponent;
	final int pageMaxSize;
	
	final int indexSizeExponent;
	final int indexMaxSize;
	
	final int maxReaders = 4;
	int maxUnzippers = 8;
	final int maxWriters = 8;
	
	final File inputFile;
	final File outputFile;
	final long totalUncompressed;
	final RagzipFileChannel walkerRagzipChannel;
	final long footerOffset;
	final Long firstExtensionOffset; 
	
	//--------
	
	Thread masterThread;
	TPool readers;
	TPool unzippers;
	TPool writers;
	ArrayBlockingQueue<Runnable> readersQ;
	ArrayBlockingQueue<Runnable> zippersQ;
	ArrayBlockingQueue<Runnable> writersQ;
	
	Exception readerFailed;
	Exception zipperFailed;
	Exception writerFailed;
	
	ThreadLocal<FileChannel> fileReadChannelTL = new ThreadLocal<>();
	ThreadLocal<FileChannel> fileWriteChannelTL = new ThreadLocal<>();
	
	Thread monitor;
	long pagesExpected;
	AtomicLong progress = new AtomicLong();
	
	//--------
	
	public RagzipParallelDecoder(File inputFile, File outputFile, int verbosity) throws IOException {
		this.inputFile = inputFile;
		this.outputFile = outputFile;
		this.verbosity = verbosity;
		this.maxUnzippers = maxUnzippers<=0 ? Runtime.getRuntime().availableProcessors() : maxUnzippers;
		
		try(FileChannel fc = FileChannel.open(inputFile.toPath(), StandardOpenOption.READ) ) {
			
			//Conveniently use the channel to read the footer and throw friendlier exceptions.
			//This ragzip channel will be closed though!!!!
			//Only its properties will be useful.
			
			this.walkerRagzipChannel = new RagzipFileChannel(fc, verbosity, 1);
			
			footerOffset = fc.size()-RagzipOutputStream.FOOTER_LENGTH64;
			firstExtensionOffset = walkerRagzipChannel.extensions.stream().findFirst().map(ext -> ext._selfOffset).orElse(null);
			
			this.pageSizeExponent = walkerRagzipChannel.pageSizeExponent;
			this.indexSizeExponent = walkerRagzipChannel.indexSizeExponent;
			this.indexMaxSize = 1 << indexSizeExponent;
			this.pageMaxSize = 1 << pageSizeExponent;
			
			this.totalUncompressed = walkerRagzipChannel.logicalSize;
			this.pagesExpected = this.totalUncompressed / pageMaxSize +1;
			
			if(pageSizeExponent<9 || pageSizeExponent>21)
				throw new IllegalArgumentException("page size exponent must be in range [9..21] (512 bytes to 2 MB)");
			if(indexSizeExponent<1 || indexSizeExponent>12)
				throw new IllegalArgumentException("index size exponent must be in range [1..12] (2 to 4096 records)");
			
			//some day, we will have to interpret walkerRagzipChannel.extensions 
		}
	}
	
	/**
	 * Start the threads and await termination of them all as they each complete their work.
	 */
	public void startDecoding() throws IOException, InterruptedException {
		masterThread = Thread.currentThread();
		
		readersQ = new ArrayBlockingQueue<>(maxReaders+20);
		zippersQ = new ArrayBlockingQueue<>(maxUnzippers+20);
		writersQ = new ArrayBlockingQueue<>(maxWriters+20);
		
		if(verbosity>0) {
			monitor = new Thread(this::monitor, "monitor");
			monitor.start();
		}
		
		
		readers = new TPool(maxReaders, readersQ, newTF("reader"));
		unzippers = new TPool(maxUnzippers, zippersQ, newTF("unzipper"));
		writers = new TPool(maxWriters, writersQ, newTF("writer"));
		
		catchInterrupt(() -> {
			if(walkerRagzipChannel.logicalSize>0) {
				if(walkerRagzipChannel.numberOfLevels>=1) {
					walkIndex(walkerRagzipChannel.numberOfLevels, walkerRagzipChannel.topIndexOffset, 0);
				} else {
					//page gz must end before next page it is level1 index
					long gzStop = firstExtensionOffset!=null ? firstExtensionOffset : footerOffset;
					//note that is ever there was a spec violation and we didn't load all extensions,
					//then the remaining extensions are still empty gzip emmbers, so it's ok to let them be read.
					
					if(verbosity>1)
						log("submitting gz offset for reading pageid 0");
					
					readers.submit(() -> readGzPage(0, gzStop, 0, (int)totalUncompressed));
					if(verbosity>1)
						log("submitted gz offset for reading pageid 0");
				}
					
			} else {
				//empty file
				writeUncompressedPage(0, ByteBuffer.allocate(0));
			}
			if(verbosity>0)
				System.out.println("Tree Walking done");
				
		});
		catchInterrupt(() -> {
			readers.shutdown();
			readers.awaitTermination();
			if(verbosity>0)
				System.out.println("Reading done");
		});
		catchInterrupt(() -> {
			unzippers.shutdown();
			unzippers.awaitTermination();
			if(verbosity>0)
				System.out.println("Unzipping done");
		});
		catchInterrupt(() -> {
			writers.shutdown();
			writers.awaitTermination();
			if(verbosity>0)
				System.out.println("Writing done");
		});
		
		if(monitor!=null)
			monitor.interrupt();
	}
	
	ThreadFactory newTF(String basename) {
		return new ClosingTF(
			basename,
			Arrays.asList(fileReadChannelTL,fileWriteChannelTL)
		);
	}
	
	void catchInterrupt(InterruptableAction action) throws IOException, InterruptedException {
		try {
			action.call();
		} catch(InterruptedException e) {
			if(readerFailed!=null || zipperFailed!=null || writerFailed!=null) {
				log("Aborting due to error(s) in thread pools");
				readers.shutdownNow();
				unzippers.shutdownNow();
				writers.shutdownNow();
				if(readerFailed!=null)
					e.addSuppressed(readerFailed);
				if(zipperFailed!=null)
					e.addSuppressed(zipperFailed);
				if(writerFailed!=null)
					e.addSuppressed(writerFailed);
				throw e;
			}
		} catch(Exception e) {
			log("Aborting due to local error(s)");
			readers.shutdownNow();
			unzippers.shutdownNow();
			writers.shutdownNow();
			throw new IOException("Failure", e);
		}
	}
	
	
	//------------
	
	void monitor() {
		String FULL = "####################";
		String EMPTY = "____________________";
		BlockingQueue<?>[] qa = {readersQ, zippersQ, writersQ};
		String[] names = {"rQ", "zQ", "wQ"};
		while(!Thread.currentThread().isInterrupted()) {
			try {
				Thread.sleep(1000);
				StringBuilder sb = new StringBuilder(""+String.format("%4.1f", 100.0*progress.get()/pagesExpected)+" %, ");
				for(int i=0; i< qa.length; i++) {
					int f = qa[i].size();
					int e = qa[i].remainingCapacity();
					int t = f+e;
					int p1 = FULL.length()*f/t;
					sb.append(names[i]).append(':').append(FULL.substring(0, p1)).append(EMPTY.substring(p1)).append('\t');
				} 
				log(sb.toString());
			} catch (InterruptedException e) {
				break;
			}
		}
	}
	
	void walkIndex(int indexLevel, long indexGzOffset, long positionBits) throws InterruptedException {
		LongBuffer indexLB;
		if(verbosity>1)
			log("walking level "+indexLevel+" position 0x"+Long.toHexString(positionBits)+" indexGzOffset="+indexGzOffset);
		
		try(FileChannel fc = FileChannel.open(inputFile.toPath(), StandardOpenOption.READ) ) {
			fc.position(indexGzOffset);
			indexLB = unwrapMetadataPayload(fc).asLongBuffer();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		if(indexLevel>1) {
			for(int i=0; i<indexLB.limit(); i++) {
				long lowerIndexGzOsffset = indexLB.get(i);
				walkIndex(indexLevel-1, lowerIndexGzOsffset, positionBits<<indexSizeExponent | i);
			}
			
		} else { //it's a level1 index containing pages
			for(int i=0; i<indexLB.limit(); i++) {
				long pageId = positionBits<<indexSizeExponent | i;
				long pageGzOsffset = indexLB.get(i);
				//page gz must end before next page it is level1 index
				long gzStop = (i+1) < indexLB.limit() ? indexLB.get(i+1) : indexGzOffset;
				
				long logicalStart = pageId << pageSizeExponent;
				long logicalEnd = Math.min(totalUncompressed, logicalStart+pageMaxSize);
				
				if(verbosity>1)
					log("submitting gz offset for reading pageid "+pageId);
				readers.submit(() -> readGzPage(pageGzOsffset, gzStop, logicalStart, (int)(logicalEnd-logicalStart)));
				if(verbosity>1)
					log("submitted gz offset for reading pageid "+pageId);
			}
		}
	}
	
	static ByteBuffer unwrapMetadataPayload(SeekableByteChannel srcChannel) throws IOException {
		ExtraField ef = ExtraField.fromGzipHead(srcChannel);
		if(ef!=null) {
			SubField sf = ef.findFirstSubField('R','A');
			if(sf!=null)
				return sf.payload;
		}
		
		throw new IOException("gzip extra subfield 'RA' not found");
	}
	
	void readGzPage(long gzStartOffset, long gzStopOffset, long logicalStart, int logicalLength) {
		try {
			long pageId = logicalStart / pageMaxSize;
			FileChannel fc = fileReadChannelTL.get();
			if(fc==null) {
				fc = FileChannel.open(inputFile.toPath(), StandardOpenOption.READ);
				fileReadChannelTL.set(fc);
			}
			
			byte[] gz = new byte[(int)(gzStopOffset-gzStartOffset)];
			ByteBuffer bb = ByteBuffer.wrap(gz);
			fc.position(gzStartOffset);
			Utils.readFully(fc, bb, () -> "premature EOS reading gz, expecting "+bb.remaining()
						+" at page id "+pageId
						+" at logical start 0x"+Long.toHexString(logicalStart)
						+" of logical length "+logicalLength
						+" gz start "+gzStartOffset
						+" gz stop "+ gzStopOffset
			);
			
			bb.flip();
			
			if(verbosity>1)
				log("submitting gz block for unzipping pageid "+pageId);
			unzippers.submit(() -> unzip(pageId, logicalStart, logicalLength, bb));
			if(verbosity>1)
				log("submitted gz block for unzipping pageid "+pageId);
			
		} catch(Exception e) {
			e.printStackTrace();
			readerFailed = e;
			masterThread.interrupt();
		}
		
	}
	
	void unzip(long pageId, long logicalStart, int logicalLength, ByteBuffer bb) {
		try {
			ByteBuffer outBB = ByteBuffer.allocate(logicalLength);
			
			BBIS bbis = new BBIS(bb);
			GZIPInputStream gzis = new GZIPInputStream(bbis, 8192);
			ReadableByteChannel rbc = Channels.newChannel(gzis);
			while(outBB.hasRemaining()) {
				int n = rbc.read(outBB);
				if(n<0)
					throw new IOException("premature EOS, expecting "+outBB.remaining()
						+" at pageid 0x"+Long.toHexString(pageId)
						+" logicalLength "+logicalLength
						+" from BB="+bb
					);
			}
			
			outBB.flip();
			if(verbosity>1)
				log("submitting block for writing pageid "+pageId);
			
			writers.submit(() -> writeUncompressedPage(logicalStart, outBB));
			if(verbosity>1)
				log("submitted block for writing pageid "+pageId);
			
		} catch (Exception e) {
			zipperFailed = e;
			e.printStackTrace();
			masterThread.interrupt();
		}
	}
	
	
	void writeUncompressedPage(long offset, ByteBuffer bb) {
		try {
			_writeSomething(offset, bb);
			progress.incrementAndGet();
		} catch(Exception e) {
			writerFailed = e;
			e.printStackTrace();
			masterThread.interrupt();
		}
	}
	
	void _writeSomething(long offset, ByteBuffer bb) throws IOException {
		FileChannel fc = fileWriteChannelTL.get();
		if(fc==null) {
			fc = FileChannel.open(outputFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
			fileWriteChannelTL.set(fc);
		}
		
		fc.position(offset);
		while(bb.hasRemaining())
			fc.write(bb);
	}
	
	
}