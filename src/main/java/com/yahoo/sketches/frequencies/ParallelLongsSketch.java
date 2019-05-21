package com.yahoo.sketches.frequencies;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ParallelLongsSketch {
	private ComposableLongsSketch global;
	private LocalSketch[] locals;
	private Merger merger;
	private int localsSize;
	private boolean shutdown = false;
	private LinkedBlockingQueue<LocalSketch> mergeQueue = new LinkedBlockingQueue<LocalSketch>();
	
	private long TEST_SIZE;
	private AtomicLong numOfUpdates = new AtomicLong(0);
	
	ParallelLongsSketch(final int numOfLocalSketches, final int maxMapSize, final int maxSketchsSize, final long TEST_SIZE){
		this.TEST_SIZE = TEST_SIZE;
		global = new ComposableLongsSketch(maxMapSize);
		localsSize = numOfLocalSketches;
		locals = new LocalSketch[localsSize];
		for (int i = 0; i < localsSize; i++) {
			locals[i] = new LocalSketch(maxMapSize, maxSketchsSize);
			locals[i].setPriority(java.lang.Thread.MAX_PRIORITY);
			locals[i].start();
		}
		merger = new Merger();
		merger.start();
	}
	
	void mergeLoacls() {
		/*for (LocalSketch localSketch : locals) {	
			while(!localSketch.dataTransferFinished.get()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					System.out.println(e.getMessage());
				}
			}
			LongsSketch temp = localSketch.updatedSketch;
			localSketch.updatedSketch = localSketch.backgroundSketch;
			localSketch.backgroundSketch = temp;
			localSketch.dataTransferFinished.set(false);
			mergeQueue.add(localSketch);
			while(!localSketch.dataTransferFinished.get()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					System.out.println(e.getMessage());
				}
			}	
		}*/
		/*
		shutdown = true;
		for (int i = 0; i < locals.length; i++) {
			try {
				locals[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println(e.getMessage());
			}	
		}
		try {
			merger.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}*/
		try {
			merger.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
	}

	public void update(final long item) {
		update(item, 1);
	}
/*	
	private int i = 0;
	public void update(final long item, final long count) {
		//locals[(int) Math.random() * localsSize].update(item, count);
		locals[i].update(item, count);
		i = (++i) % localsSize;
	}
*/
	public void update(final long item, final long count) {
		locals[(int) Math.random() * localsSize].update(item, count);
	}
	
	public long getStreamLength() {
		return global.getStreamLength();
	}
	
	public long getMaximumError() {
		return global.getMaximumError();
	}
	
	public long getEstimate(final long item) {
		return global.getEstimate(item);
	}
	
	public long getLowerBound(final long item) {
		return global.getLowerBound(item);
	}
	
	public LongsSketch.Row[] getFrequentItems(final long threshold, final ErrorType errorType){
		return global.getFrequentItems(threshold, errorType);
	}
	
	public LongsSketch.Row[] getFrequentItems(final ErrorType errorType){
		return global.getFrequentItems(errorType);
	}
	
	private class Merger extends Thread {
		@Override
		public void run() {
			LocalSketch curr;
			while (!shutdown) {
				try {
					curr = mergeQueue.take();
				} catch (InterruptedException e) {
					System.out.println(e.getMessage());
					continue;
				}
				global.merge(curr.backgroundSketch);
				if(global.getStreamLength() >= TEST_SIZE) {
					shutdown = true;
					for (int i = 0; i < locals.length; i++) {
						try {
							locals[i].join();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							
							System.out.println(e.getMessage());
						}	
					}
					break;
				}
				curr.backgroundSketch.reset();
				curr.dataTransferFinished.set(true);
				synchronized (curr.mergingLock) {
					curr.mergingLock.notify();
				}
			}
			System.out.println("numOfUpdates is: " + numOfUpdates.get());
		}
	}
	
	private class LocalSketch extends Thread {
		private LongsSketch updatedSketch;
		private LongsSketch backgroundSketch;	
		private int LocalSketchsMaxSize;
		private int currentSkecthUpdates = 0;
		private AtomicBoolean dataTransferFinished = new AtomicBoolean(true);

		public Object mergingLock = new Object();
		public LinkedBlockingQueue<longPair> stream = new LinkedBlockingQueue<longPair>();
		
		LocalSketch(final int maxMapSize, final int maxSketchsSize){
			LocalSketchsMaxSize = maxSketchsSize;
			updatedSketch = new LongsSketch(maxMapSize);
			backgroundSketch = new LongsSketch(maxMapSize);			
		}
		
		public void update(final long item, final long count) {
			stream.add(new longPair(item, count));
		}
		
		@Override
		public void run() {
			longPair pair = new longPair(0, 1);
			while (!shutdown){
				/*try {
					pair = stream.take();
				} catch (InterruptedException e) {
					System.out.println(e.getMessage());
					continue;
				}*/
				numOfUpdates.incrementAndGet();
				internalUpdate(pair.A, pair.B);
			}	
		}
		
		private void internalUpdate(long item, final long count) {
			item = (long) (Math.random() * 1000000);
			updatedSketch.update(item, count);
			currentSkecthUpdates += count;
			if(currentSkecthUpdates >= LocalSketchsMaxSize) {
				while(!dataTransferFinished.get()) {
					synchronized (mergingLock) {
					    try {
					    	System.out.println("b is not big enough");
							mergingLock.wait();
						} catch (InterruptedException e) {
							System.out.println(e.getMessage());
						}
					}
				}
				currentSkecthUpdates = 0;
				LongsSketch temp = updatedSketch;
				updatedSketch = backgroundSketch;
				backgroundSketch = temp;
				dataTransferFinished.set(false);
				mergeQueue.add(this);	
			}		
		}
	}
	private static class longPair{
		public long A;
		public long B;
		
		longPair(long A, long B){
			this.A = A;
			this.B = B;
		}
	}
}
