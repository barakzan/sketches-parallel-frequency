package com.yahoo.sketches.frequencies;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParallelLongsSketch {
	private ComposableLongsSketch global;
	private LocalSketch[] locals;
	private int localsSize;
	private Merger merger;
	private LinkedBlockingQueue<LocalSketch> mergeQueue = new LinkedBlockingQueue<LocalSketch>();
	
	ParallelLongsSketch(final int numOfLocalSketches, final int maxMapSize, final int maxSketchsSize){
		global = new ComposableLongsSketch(maxMapSize);
		localsSize = numOfLocalSketches;
		locals = new LocalSketch[localsSize];
		for (int i = 0; i < localsSize; i++) {
			locals[i] = new LocalSketch(maxMapSize, maxSketchsSize);
			locals[i].start();
		}
		merger = new Merger();
		merger.start();
	}
	
	void mergeLoacls() {
		for (LocalSketch localSketch : locals) {
			while(!localSketch.dataTransferFinished.get()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
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
					e.printStackTrace();
				}
			}	
		}
		for (int i = 0; i < locals.length; i++) {
			locals[i] = null;	
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
			while (true) {
				try {
					curr = mergeQueue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
					continue;
				}
				global.merge(curr.backgroundSketch);
				curr.backgroundSketch.reset();
				curr.dataTransferFinished.set(true);
				synchronized (curr.mergingLock) {
					curr.mergingLock.notify();
				}
			}
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
			longPair pair;
			while (true){
				try {
					pair = stream.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
					continue;
				}
				internalUpdate(pair.A, pair.B);
			}	
		}
		
		private void internalUpdate(final long item, final long count) {
			updatedSketch.update(item, count);
			currentSkecthUpdates += count;
			if(currentSkecthUpdates >= LocalSketchsMaxSize) {
				while(!dataTransferFinished.get()) {
					synchronized (mergingLock) {
					    try {
							mergingLock.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
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
