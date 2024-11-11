/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import static com.github.ddeschenes_1.ragzip.Utils.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import com.github.ddeschenes_1.ragzip.Utils.BBOS;
import com.github.ddeschenes_1.ragzip.Utils.ClosingTF;
import com.github.ddeschenes_1.ragzip.Utils.InterruptableAction;
import com.github.ddeschenes_1.ragzip.Utils.TPool;

/**
 * Compress an input file into a ragzip file, using a pipeline of multi threaded stages.
 * There a 5 stages: slicing, reading, zipping, indexing (ordering), writing.
 * There are 4 queues: rQ, zQ, iQ and wQ which are reported by the monitor every seconds, along with progress. 
 * <p>
 * This tool dot not read stdin since it requires direct input file access.
 * <p>
 * The maximum page size exponents supported is 21 (2^21 = 2MB) because pages are loaded in memory.
 */
public class RagzipParallelEncoder {
	
	int verbosity = 0;
	
	final int pageSizeExponent;
	final int pageMaxSize;
	
	final int indexSizeExponent;
	final int indexMaxSize;
	
	final int maxReaders = 4;
	int maxZippers = 8;
	final int maxBufferedPages = 20;
	final int maxWriters = 8;
	
	final File inputFile;
	final File outputFile;
	final long totalUncompressed;
	
	//--------
	
	int[] levelsOccupancies = new int[54]; //[0] is never used, 1..53
	ByteBuffer[] indexes = new ByteBuffer[54];//[0] is never used, 1..53

	
	Thread masterThread;
	TPool readers;
	TPool zippers;
	Thread indexer;
	TPool writers;
	volatile boolean indexerShouldFinish = false;
	ArrayBlockingQueue<Runnable> readersQ;
	ArrayBlockingQueue<Runnable> zippersQ;
	PriorityBlockingQueue<Page> indexerQ;
	ArrayBlockingQueue<Runnable> writersQ;
	
	final ReentrantLock indexerQStateLock = new ReentrantLock();
	final Condition indexerQNotFullCondition = indexerQStateLock.newCondition();
	final Condition indexerQNotEmptyCondition = indexerQStateLock.newCondition();
	long awaitedPageId = 0;
	
	Exception readerFailed;
	Exception zipperFailed;
	Exception indexerFailed;
	Exception writerFailed;
	
	ThreadLocal<FileChannel> fileReadChannelTL = new ThreadLocal<>();
	ThreadLocal<FileChannel> fileWriteChannelTL = new ThreadLocal<>();
	
	Thread monitor;
	long pagesExpected;
	AtomicLong progress = new AtomicLong();
	
	//--------
	
	public RagzipParallelEncoder(File inputFile, File outputFile, int pageSizeExponent, int indexSizeExponent) {
		if(pageSizeExponent<9 || pageSizeExponent>21)
			throw new IllegalArgumentException("page size exponent must be in range [9..21] (512 bytes to 2 MB)");
		if(indexSizeExponent<1 || indexSizeExponent>12)
			throw new IllegalArgumentException("index size exponent must be in range [1..12] (2 to 4096 records)");
		
		this.inputFile = inputFile;
		this.outputFile = outputFile;
		this.pageSizeExponent = pageSizeExponent;
		this.indexSizeExponent = indexSizeExponent;
		this.indexMaxSize = 1 << indexSizeExponent;
		this.pageMaxSize = 1 << pageSizeExponent;
		
		long max = 1L<<62;
		this.totalUncompressed = inputFile.length();
		this.pagesExpected = this.totalUncompressed / pageMaxSize +1;
		if(totalUncompressed > max) //exactly at max, the unique full level2 still fits in footer, something the RagzipOutputStream cannot do because it allocates the next page early. 
			throw new IllegalArgumentException("this class cannot encode files bigger than "+max+" bytes (0x"+Long.toHexString(max)+")");
		
		this.maxZippers = maxZippers<=0 ? Runtime.getRuntime().availableProcessors() : maxZippers;
	}
	
	public RagzipParallelEncoder withVerbosity(int verbosity1) {
		this.verbosity = verbosity1;
		return this;
	}
	
	/**
	 * Start the threads and await termination of them all as they each complete their work.
	 */
	public void startEncoding() throws InterruptedException, IOException {
		masterThread = Thread.currentThread();
		
		readersQ = new ArrayBlockingQueue<>(maxReaders+20);
		zippersQ = new ArrayBlockingQueue<>(maxZippers+20);
		indexerQ = new PriorityBlockingQueue<>(maxBufferedPages);
		writersQ = new ArrayBlockingQueue<>(maxWriters+20);
		
		if(verbosity>0) {
			monitor = new Thread(this::monitor, "monitor");
			monitor.start();
		}
		
		readers = new TPool(maxReaders, readersQ, newTF("reader"));
		zippers = new TPool(maxZippers, zippersQ, newTF("zipper"));
		indexer = newTF("indexer").newThread(this::indexingLoop);
		indexer.start();
		writers = new TPool(maxWriters, writersQ, newTF("writer"));
		
		catchInterrupt(() -> {
			slice();
			if(verbosity>0)
				System.out.println("Slicing done");
		});
		catchInterrupt(() -> {
			readers.shutdown();
			readers.awaitTermination();
			if(verbosity>0)
				System.out.println("Reading done");
		});
		catchInterrupt(() -> {
			zippers.shutdown();
			zippers.awaitTermination();
			if(verbosity>0)
				System.out.println("Zipping done");
		});
		catchInterrupt(() -> {
			indexerShouldFinish = true;
			indexerQStateLock.lock();
			try {
				indexerQNotEmptyCondition.signalAll();
			} finally {
				indexerQStateLock.unlock();
			}
			indexer.join();
			if(verbosity>0)
				System.out.println("Indexing done");
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
	
	void catchInterrupt(InterruptableAction action) throws InterruptedException, IOException {
		try {
			action.call();
		} catch(InterruptedException e) {
			if(readerFailed!=null || zipperFailed!=null || indexerFailed!=null || writerFailed!=null) {
				log("Aborting due to error(s) in thread pools");
				readers.shutdownNow();
				zippers.shutdownNow();
				indexerShouldFinish = true;
				indexer.interrupt();
				writers.shutdownNow();
				if(readerFailed!=null)
					e.addSuppressed(readerFailed);
				if(zipperFailed!=null)
					e.addSuppressed(zipperFailed);
				if(indexerFailed!=null)
					e.addSuppressed(indexerFailed);
				if(writerFailed!=null)
					e.addSuppressed(writerFailed);
				throw e;
			}
		} catch(Exception e) {
			log("Aborting due to local error(s)");
			readers.shutdownNow();
			zippers.shutdownNow();
			indexerShouldFinish = true;
			indexer.interrupt();
			writers.shutdownNow();
			throw new IOException(e);
		}
	}
	
	
	//------------
	
	void monitor() {
		String FULL = "####################";
		String EMPTY = "____________________";
		BlockingQueue<?>[] qa = {readersQ, zippersQ, indexerQ, writersQ};
		String[] names = {"rQ", "zQ", "iQ", "wQ"};
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
	
	void slice() throws InterruptedException {
		for(long pageLogicalOffset=0; pageLogicalOffset<totalUncompressed; pageLogicalOffset+= pageMaxSize) {
			long pageId = pageLogicalOffset/pageMaxSize;
			int len = (int) Math.min(totalUncompressed-pageLogicalOffset, pageMaxSize);
			
			indexerQStateLock.lock();
			try {
				while(pageId >= awaitedPageId + maxBufferedPages) {
					if(verbosity>1)
						log("waiting indexer not full before pageid "+pageId);
					indexerQNotFullCondition.await();
				}
			} finally {
				indexerQStateLock.unlock();
			}
			
			long from = pageLogicalOffset;
			
			if(verbosity>1)
				log("submitting slice pageid "+pageId);
			readers.submit(() -> readPartition(pageId, from, len));
			if(verbosity>1)
				log("submitted slice pageid "+pageId);
		}
	}
	
	void readPartition(long pageId, long logicalStart, int logicalLength) {
		try {
			FileChannel fc = fileReadChannelTL.get();
			if(fc==null) {
				fc = FileChannel.open(inputFile.toPath(), StandardOpenOption.READ);
				fileReadChannelTL.set(fc);
			}
			
			byte[] ba = new byte[logicalLength];
			ByteBuffer bb = ByteBuffer.wrap(ba);
			fc.position(logicalStart);
			Utils.readFully(fc, bb, () -> "EOF while reading page at 0x"+Long.toHexString(logicalStart));
			bb.flip();
			
			if(verbosity>1)
				log("submitting read block for pageid "+pageId);
			zippers.submit(() -> zip(pageId, logicalStart, logicalLength, bb));
			if(verbosity>1)
				log("submitted read block for pageid "+pageId);
			
		} catch(Exception e) {
			e.printStackTrace();
			readerFailed = e;
			masterThread.interrupt();
		}
		
	}
	
	void zip(long pageId, long logicalStart, int logicalLength, ByteBuffer bb) {
		try {
			ByteBuffer outBB = ByteBuffer.allocate(20 + 4*bb.limit()/3);//133% is deemed the max worst compression
			BBOS bbos = new BBOS(outBB);
			GZIPOutputStream gzos = new GZIPOutputStream(bbos, 8192);
			WritableByteChannel wbc = Channels.newChannel(gzos);
			
			while(bb.hasRemaining()) {
				wbc.write(bb);
			}
			
			gzos.flush();
			gzos.finish();
			gzos.close();
			
			outBB.flip();
			if(verbosity>1)
				log("submitting to indexer zipped pageid "+pageId);
			//in theory these put() should never block because the indexerQ backpressure is felt in the slicer
			//and the indexerQ is always larger than the maxBufferedPage
			//to detect anomalies, the offer() is used in order to avoid blocking and fail if not accepted.
			//indexerQ.put(new Page(pageId, logicalStart, logicalLength, outBB, outBB.limit()));
			boolean accepted = indexerQ.offer(new Page(pageId, logicalStart, logicalLength, outBB, outBB.limit()));
			if(!accepted)
				throw new IllegalStateException("the zipper got rejected by the indexerQ! this should never happen! fix me!");
			
			indexerQStateLock.lock();
			try {
				indexerQNotEmptyCondition.signalAll();
			} finally {
				indexerQStateLock.unlock();
			}
			if(verbosity>1)
				log("submitted to indexer zipped pageid "+pageId);
			
		} catch (Exception e) {
			zipperFailed = e;
			e.printStackTrace();
			masterThread.interrupt();
		}
	}
	
	
	void indexingLoop() {
		awaitedPageId = 0;
		long nextGzipOffset = 0;
		
		indexerQStateLock.lock();
		try {
			while (!indexerQ.isEmpty() || !indexerShouldFinish) {
				Page pg = indexerQ.peek();
				if(pg==null || pg.pageId > awaitedPageId) {
					indexerQNotEmptyCondition.await();
				} else {
					indexerQ.take(); //same page, won't block
					
					if(verbosity>1)
						log("submitting to writer pageid "+pg.pageId);
					long pageOffset = nextGzipOffset;
					writers.submit(() -> writePage(pageOffset, pg));
					if(verbosity>1)
						log("submitted to writer pageid "+pg.pageId);
					
					nextGzipOffset += pg.zippedLength;
					nextGzipOffset = addRecord(pageOffset, nextGzipOffset, 1);
					
					awaitedPageId++;
					indexerQNotFullCondition.signalAll();
				}
			}
			
			flushFooter(nextGzipOffset);
			
		} catch (Exception e) {
			indexerFailed = e;
			e.printStackTrace();
			masterThread.interrupt();
			
		} finally {
			indexerQStateLock.unlock();
		}
	}
	
	/**
	 * Adds an offset to an index of given level.
	 * If the level never existed, a bytebuffer is created once and reused later.
	 * 
	 * We write the level's index when its occupancy == indexMaxSize,
	 * then we record this index in the higher level (which in turn may flush and so on).
	 * 
	 * @return the next gzip offset where the caller can resume appending pages or indexes. 
	 */
	private long addRecord(long recordOffset, long nextGzipOffset, int level) throws IOException {
		if(indexes[level]==null) { //for sure levelsOccupancies[level]==0
			indexes[level] = ByteBuffer.allocate(8*indexMaxSize);
			
		} else if(levelsOccupancies[level]==indexMaxSize) {
			long indexOffset = nextGzipOffset;
			
			nextGzipOffset += writeEmptyGzipWithMetaData(indexOffset, indexes[level].flip());
			levelsOccupancies[level] = 0;
			indexes[level].clear(); //ready for reuse
			
			nextGzipOffset = addRecord(indexOffset, nextGzipOffset, level+1); //cascade, recursive call 
		}
		
		indexes[level].putLong(recordOffset);
		levelsOccupancies[level]++;
		
		return nextGzipOffset;
	}
	
	private void flushFooter(long nextGzipOffset) throws IOException {
		
		/*
		 * In the case of uncompressed content < 1 full page, the parallel encoder still sent a page
		 * (albeit partial) and the sequencer had to record 1 page in the index 1.
		 * It doesn't know that there are no further pages
		 * (well, it does if page size < pageMaxSize, but not if == pageMxSize).
		 * 
		 * Fortunately the sequencer won't create an empty index after a roll over since it only
		 * rolls over when there is a certainty that another offset must be recorded.
		 * 
		 * The difference with the RagzipOutputStream is in that knowledge of the last full or partial page.
		 * 
		 * So, to be able to encode a numberOfLevels==0, we must examine the index occupancy
		 * and if it is == 1, then take only that offset as the topIndexOffset for the footer.
		 */
		int numberOfLevels = indexes.length-1;
		while(numberOfLevels>0 && indexes[numberOfLevels]==null) { 
			numberOfLevels--;
		}
		
		long topIndexOffset = 0;
		
		if(numberOfLevels==1 && levelsOccupancies[1]==1) {
			//only one page, skip level 1.
			numberOfLevels = 0;
			
		} else {
			//--- flush every partial or full index
			
			for(int level=1; level<indexes.length; level++) { //we don't index at level0, the page is level 0.
				if(indexes[level]==null)
					break;
				//found a partial or full level, time to flush it.
				numberOfLevels = level; //provisional watermark
				
				long indexOffset = nextGzipOffset;
				topIndexOffset  = indexOffset; //provisional watermark
				
				nextGzipOffset += writeEmptyGzipWithMetaData(indexOffset, indexes[level].flip());
				if(indexes[level+1]!=null) { //there are higher levels
					nextGzipOffset = addRecord(indexOffset, nextGzipOffset, level+1); //cascade, recursive call
				}
			}
		}
		
		
		
		//---- append extensions -----
		
		long previousExtensionsOffset = -1;
		
		//---- append footer ----
		
		ByteBuffer footerBB = ByteBuffer.allocate(100);
		footerBB.putInt(RagzipOutputStream.VERSION_1_0); 
		int treeSpec = (numberOfLevels<<16) | (indexSizeExponent<<8) | pageSizeExponent; //reserved, number of index levels, index size exp, page size exp
		footerBB.putInt(treeSpec);
		footerBB.putLong(totalUncompressed);
		footerBB.putLong(topIndexOffset); //top index offset is an index1 type, because the treeSpec said so.
		footerBB.putLong(previousExtensionsOffset); //last extension offset 
		footerBB.put(new byte[6]); //padding for this deflate implementation which uses 2 bytes only for an empty deflate stream.
		
		if(verbosity>1)
			log("flushing footer at "+nextGzipOffset);
		writeEmptyGzipWithMetaData(nextGzipOffset, footerBB.flip());
	}
	
	
	private long writeEmptyGzipWithMetaData(long nextGzipOffset, ByteBuffer metadata) throws IOException {
		ByteBuffer mdgz = GzipUtils.writeEmptyGzipRAMetadata(metadata);
		_writeSomething(nextGzipOffset, mdgz);
		return mdgz.limit();
	}
	
	
	void writePage(long offset, Page pg) {
		try {
			if(verbosity>1)
				log("writing page at offset "+offset);
			_writeSomething(offset, pg.gz);
			progress.incrementAndGet();
		} catch(Exception e) {
			writerFailed = e;
			e.printStackTrace();
			masterThread.interrupt();
		}
	}
	
	private void _writeSomething(long offset, ByteBuffer bb) throws IOException {
		FileChannel fc = fileWriteChannelTL.get();
		if(fc==null) {
			fc = FileChannel.open(outputFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
			fileWriteChannelTL.set(fc);
		}
		
		fc.position(offset);
		while(bb.hasRemaining())
			fc.write(bb);
	}
	
	
	//=============
	
	
	record Page(long pageId, long logicalStart, int logicalLength, ByteBuffer gz, long zippedLength) implements Comparable<Page> {
		@Override
		public int compareTo(Page o) {
			return Long.compare(this.pageId, o.pageId);
		}
	}
	
	
}