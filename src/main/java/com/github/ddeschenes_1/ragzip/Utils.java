/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Utils {
	/**
	 * Simple System.out.println() of a msg.toString() prefixed with a date and the thread name.
	*/
	public static void log(Object msg) {
		System.out.println(""+new Date()+": ["+Thread.currentThread().getName()+"] "+msg);
	}
	
	
	static void readFully(ReadableByteChannel fromRbc, ByteBuffer toBB, Supplier<String> eofMsg) throws IOException {
		while (toBB.hasRemaining() && fromRbc.read(toBB) >= 0) {//
		}
		if (toBB.hasRemaining())
			throw new EOFException(eofMsg!=null ? eofMsg.get() : null);
	}
	
	static void writeFully(ByteBuffer fromBB, WritableByteChannel toWbc) throws IOException {
		while (fromBB.hasRemaining()) {
			toWbc.write(fromBB);
		}
	}
	
	
	
	interface InterruptableAction {
		void call() throws InterruptedException, IOException;
	}
	
	
	static class BBIS extends InputStream {
		ByteBuffer bb;
		BBIS(ByteBuffer bb) {
			this.bb = bb;
		}
		
		@Override
		public int read() throws IOException {
			return !bb.hasRemaining() ? -1 : bb.get() & 0xFF;
		}
		
		@Override
		public int read(byte[] bytes, int off, int len) throws IOException {
			if (!bb.hasRemaining())
				return -1;
			len = Math.min(len, bb.remaining());
			bb.get(bytes, off, len);
			return len;
		}
	}
	
	static class BBOS extends OutputStream {
		final ByteBuffer bb;
		
		BBOS(ByteBuffer bb) {
			this.bb = bb;
		}
		
		@Override
		public void write(int b) throws IOException {
			bb.put((byte)b);
		}
		
		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			bb.put(b, off, len);
		}
	}
	
	static class TwoPhaseNoCloseBufferInputStream extends BufferedInputStream {
		TwoPhaseNoCloseBufferInputStream(InputStream in, int buffsize1, int buffsize2) throws IOException {
			super(in, buffsize2);
			int n = in.read(buf, 0, buffsize1);
			count = n;
		}

		@Override
		public void close() throws IOException {
			//no-op
		}
		
		InputStream getDelegate() {
			return in;
		}
	}
	
	static class NoCloseInputStream extends FilterInputStream {
		NoCloseInputStream(InputStream in) {
			super(in);
		}

		@Override
		public void close() throws IOException {
			//no-op
		}
		
		InputStream getDelegate() {
			return in;
		}
	}
	
	static class NoCloseOutputStream extends FilterOutputStream { //MUST BE UNBUFFERED
		
		NoCloseOutputStream(OutputStream out) {
			super(out);
		}

		@Override
		public void close() throws IOException {
			out.flush();
			//no-op
		}
		
		OutputStream getDelegate() {
			return out;
		}
	}
	
	static class NoCloseChannel implements SeekableByteChannel {
		SeekableByteChannel delegate;
		
		NoCloseChannel(SeekableByteChannel delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public boolean isOpen() {
			return delegate.isOpen();
		}
		
		@Override
		public void close() throws IOException {
			//delegate.close();
		}
		
		@Override
		public int read(ByteBuffer dst) throws IOException {
			return delegate.read(dst);
		}
		
		@Override
		public long position() throws IOException {
			return delegate.position();
		}
		
		@Override
		public SeekableByteChannel position(long newPosition) throws IOException {
			return delegate.position(newPosition);
		}
		
		@Override
		public long size() throws IOException {
			return delegate.size();
		}
		
		//-----
		
		@Override
		public int write(ByteBuffer src) throws IOException {
			throw new NonWritableChannelException();
		}
		
		@Override
		public SeekableByteChannel truncate(long size) throws IOException {
			throw new NonWritableChannelException();
		}
	}
	
	static class TPool {
		static Runnable END = () -> {/**/};
		
		Thread[] threadArray;
		BlockingQueue<Runnable> bQueue;
		volatile boolean finish = false;
		volatile boolean die = false;
		CountDownLatch allDeadCdl;
		
		TPool(int n, BlockingQueue<Runnable> q, ThreadFactory tf) {
			this.bQueue = q;
			this.threadArray = new Thread[n];
			this.allDeadCdl = new CountDownLatch(n);
			for(int i=0; i<n; i++) {
				threadArray[i] = tf.newThread(() -> runDequeuer());
				threadArray[i].start();
			}
		}
		
		void shutdown() {
			this.finish = true;
			bQueue.offer(END); //if queue is empty, this will nudge the worker
		}
		
		void shutdownNow() {
			this.finish = true;
			bQueue.offer(END); //if queue is empty, this will nudge the worker
			Stream.of(threadArray).forEach(t -> t.interrupt());
		}
		
		void runDequeuer() {
			try {
				while(!Thread.currentThread().isInterrupted()) {
					try {
						Runnable r = finish ? bQueue.poll() : bQueue.poll(1, TimeUnit.SECONDS);
						if(r==END) {
							bQueue.put(END); //notify another worker
							break;
						}
						
						if(r!=null) {
							r.run();
						} else if(finish) {
							break;
						}
					} catch(InterruptedException e) {
						break;
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
			} finally {
				allDeadCdl.countDown();
			}
		}
		
		void submit(Runnable r) throws InterruptedException {
			bQueue.put(r);
		}
		
		void awaitTermination() throws InterruptedException {
			allDeadCdl.await();
		}
	}
	
	
	/**
	 * Thread factory that finally closes (on thread termination after the run() is finished)
	 * any non-null Closeable seen from the ThreadLocal<? extends Closeable> given.
	 * 
	 * This is typically caching closeable things that should end with the thread.
	 * Since the runnable may be a loop in a thread pool, such threadlocals may only be closed when the pool shuts down.
	 */
	static class ClosingTF implements ThreadFactory {
		AtomicInteger nextId = new AtomicInteger();
		String baseName;
		
		List<ThreadLocal<? extends Closeable>> closeableTLs = new ArrayList<>();
		
		ClosingTF(String baseName, List<ThreadLocal<? extends Closeable>> closeableTLs) {
			this.baseName = baseName;
			this.closeableTLs = closeableTLs;
		}
		
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(() -> {
				try {
					r.run();
				} finally {
					for(ThreadLocal<? extends Closeable> cTL: closeableTLs) {
						try {
							Closeable c = cTL.get();
							if(c!=null)
								c.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}, baseName+"-"+(nextId.getAndIncrement()));
		}
	}
}
