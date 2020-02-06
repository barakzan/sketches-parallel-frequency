package com.yahoo.sketches.frequencies;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParallelLongsSketch {
	private ComposableLongsSketch global;
	private LocalSketch[] locals;
	private Merger merger;
	private int localsSize;
	private boolean optimizeMerge;
	private LinkedBlockingQueue<LocalSketch> mergeQueue = new LinkedBlockingQueue<LocalSketch>();
	
	private TestTypes testType;
	private long testSize;
	private int randomRange;
	private long startParalleTime;
	
	public enum TestTypes
	{
		NO_TEST,
		TEST_ZEROS,
		TEST_RANDOM_RANGE,
		TEST_SEQUENTIAL_NUMBERS
	}
	
	ParallelLongsSketch(){
		initParallelLongsSketch(7, 256, 10000000, TestTypes.NO_TEST, true, 0L, 10000);
	}
	
	ParallelLongsSketch(int numOfLocalSketches, int maxMapSize, int maxSketchsSize)
	{
		initParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize, TestTypes.NO_TEST, true, 0L, 10000);
	}

	ParallelLongsSketch(int numOfLocalSketches, int maxMapSize, int maxSketchsSize, TestTypes testType, boolean optimizeMerge, long testSize){
		initParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize, testType, optimizeMerge, testSize, 10000);
	}
	
	ParallelLongsSketch(int numOfLocalSketches, int maxMapSize, int maxSketchsSize, TestTypes testType, boolean optimizeMerge, long testSize, int testRandomRange){
		initParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize, testType, optimizeMerge, testSize, testRandomRange);
	}
	
	private void initParallelLongsSketch(int numOfLocalSketches, int maxMapSize, int maxSketchsSize, TestTypes testType, boolean optimizeMerge, long testSize, int testRandomRange) {
		this.testType = testType;
		this.testSize = testSize;
		this.optimizeMerge = optimizeMerge;
		randomRange = testRandomRange;
		localsSize = numOfLocalSketches;
		locals = new LocalSketch[localsSize];
		for (int i = 0; i < localsSize; i++) {
			locals[i] = new LocalSketch(maxMapSize, maxSketchsSize);
		}
		global = new ComposableLongsSketch(maxMapSize);
		merger = new Merger();
		
		startParalleTime = System.currentTimeMillis();
		for (int i = 0; i < localsSize; i++) {
			locals[i].start();
		}	
		merger.start();
	}
	
	public void mergeLocals() {
		for (int i = 0; i < localsSize; i++) {
			if (locals[i].isAlive() && !locals[i].isDone) {
				locals[i].stream.add(new longPair(-1, -1));
			}
		}	
	}
	
	public long finishThenDispose() {
		if (testType == TestTypes.NO_TEST) {
			int finished = 0;
			int i = 0;
			while (finished < localsSize) {
				long currStreamSize = locals[i].stream.size();
				long currentSkecthUpdates = locals[i].currentSkecthUpdates;
				boolean dataTransferFinished = locals[i].dataTransferFinished.get();
				if (currStreamSize == 0 && currentSkecthUpdates == 0 &&	dataTransferFinished) {
					finished++;
				} else {
					finished = 0;
				}
				if (i == localsSize - 1) {
					mergeLocals();
					
						sleep(200);
				}
				i = (i+1) % localsSize;
			}
			
			merger.shutdown = true;
			for (LocalSketch localSketch : locals) {
				localSketch.shutdown = true;
			}

			mergeLocals();
		}
		disposeNow();
		return System.currentTimeMillis() - startParalleTime;
	}
	
	private void disposeNow() {
		try {
			//mergeQueue.add(new LocalSketch());
			merger.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void update(final long item) {
		update(item, 1);
	}

	private int currentLocal = 0;
	public void update(final long item, final long count) {
		locals[currentLocal].update(item, count);
		currentLocal = (currentLocal + 1) % localsSize;
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
		public boolean shutdown = false;
		
		Merger() {
			setName("Merger");
		}

		@Override
		public void run() {
			try {
				LocalSketch curr;
				while (!shutdown) {
					curr = mergeQueue.take();		
					if (curr.isFake) {
						break;
					}		
					global.merge(curr.backgroundSketch);
	
					if(testType != TestTypes.NO_TEST && global.getStreamLength() >= testSize) {
						break;
					}
	
					curr.backgroundSketch.reset();		
					curr.dataTransferFinished.set(true);
					
					synchronized (curr.mergingLock) {
						curr.mergingLock.notify();
					}
				}
				
				for (LocalSketch localSketch : locals) {
					if (localSketch != null) {
						localSketch.shutdown = true;
					}
				}
				mergeLocals();
				for (int i = 0; i < locals.length; i++) {
					curr = locals[i];
					if (curr.isAlive() && !curr.isDone) {
						curr.dataTransferFinished.set(true);
						synchronized (curr.mergingLock) {
							curr.mergingLock.notify();
						}
					}
				}	
				for (int i = 0; i < locals.length; i++) {
					locals[i].join();
				}
			} catch (InterruptedException e) {
				System.out.println(e.getMessage());
			}
		}	
	}
	
	private class LocalSketch extends Thread {
		private LongsSketch updatedSketch;
		private LongsSketch backgroundSketch;	
		private int MaxLocalSketchsSize;
		private int CurLocalSketchsSize = 4;
		private int currentSkecthUpdates = 0;
		private boolean isFake = false;
		private boolean isDone = false;
		private AtomicBoolean dataTransferFinished = new AtomicBoolean(true);

		public boolean shutdown = false;
		public Object mergingLock = new Object();
		public LinkedBlockingQueue<longPair> stream = new LinkedBlockingQueue<longPair>();
			
		LocalSketch(final int maxMapSize, final int maxSketchsSize){
			MaxLocalSketchsSize = maxSketchsSize;
			updatedSketch = new LongsSketch(maxMapSize);
			backgroundSketch = new LongsSketch(maxMapSize);
			setName("LocalSketch");
		}
		
		public void update(final long item, final long count) {
			stream.add(new longPair(item, count));
		}
		
		@Override
		public void run() {
			try {
				switch (testType) {	
				case TEST_ZEROS:
					//// test with zeros
					while (!shutdown){
						internalUpdate(0, 1);
					}	
					break;
					
				case TEST_RANDOM_RANGE:
					//// test with random numbers
					Random myRandom = new Random();
					while (!shutdown){
						internalUpdate(myRandom.nextInt(randomRange), 1);
					}	
					break;
				case TEST_SEQUENTIAL_NUMBERS:
					//// test with sequential numbers
					long i = 0;
					while (!shutdown){
						internalUpdate(++i, 1);
					}	
					break;
				default:
					longPair pair;
					while (!shutdown){
						pair = stream.take();
						if (pair.B == -1) {
							merge();
						}
						else {
							internalUpdate(pair.A, pair.B);	
						}	
					}
					break;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			isDone = true;
		}
		
		private void merge() {
			while(!dataTransferFinished.get()) {
				//System.out.println("b is not big enough " + Thread.currentThread().getId());
				waitOnLock(mergingLock);
			}
			if (optimizeMerge) {
				// copy the keys from the full map to the new empty map with zero values. 
				backgroundSketch.setHashMap(updatedSketch.getHashMap().getMapCopyWithZeroValues());
			}
			
			// swap sketches
			LongsSketch temp = backgroundSketch;
			backgroundSketch = updatedSketch;
			updatedSketch = temp;
						
			currentSkecthUpdates = 0;
			dataTransferFinished.set(false);
			mergeQueue.add(this);
		}
		
		private void internalUpdate(long item, final long count) {
			updatedSketch.update(item, count);
			currentSkecthUpdates += count;
			if(currentSkecthUpdates >= CurLocalSketchsSize) {
				if(CurLocalSketchsSize < MaxLocalSketchsSize) {
					CurLocalSketchsSize <<= 1; // *= 2;
					if (CurLocalSketchsSize > MaxLocalSketchsSize) {
						CurLocalSketchsSize = MaxLocalSketchsSize;
					}
				}
				merge();
			}	
		}
		
		private void waitOnLock(Object lock) {
			synchronized (lock) {
			    try {
			    	lock.wait();
				} catch (InterruptedException e) {
					System.out.println(e.getMessage());
				}
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
	
	  public void sleep(int time) {
		  	try {
				Thread.sleep(time);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("sleeping for " + time + "ms");
		  }
}
