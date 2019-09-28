/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import org.testng.annotations.Test;
import com.yahoo.sketches.frequencies.LongsSketch.Row;

public class ComposableLongsSketchTest {
	
public static void main(String[] args) {
	println("Main started");
	ComposableLongsSketchTest t1 = new ComposableLongsSketchTest();
	//t1.basicParallelLongsSketchTest();
	t1.ParallelLongsSketchSpeedTest();
	
	////ComposableLongsSketchTest t = new ComposableLongsSketchTest();
	////t.basicComposableLongsSketchTest();
}

  @Test
  public void ParallelLongsSketchSpeedTest() {
	  	int numOfLocalSketches = 4;
	  	int maxMapSize = 32;
	  	int maxSketchsSize = 1000000;
	  	long numOfInputs = 100000000L; 
	  	long smallRandomRange = maxMapSize + 7;
	  	long bigRandomRange = 100000;
	  	long randLong;
	  	
	  	System.out.println("ParallelLongsSketchSpeedTest started\n");
	  	
	  	System.out.println("numOfLocalSketches - " + numOfLocalSketches);
	  	System.out.println("maxMapSize - " + maxMapSize);
	  	System.out.println("maxSketchsSize - " + maxSketchsSize);
	  	System.out.println("numOfInputs - " + numOfInputs);
	  	System.out.println();	
	  	
		//test new with zeros
		ParallelLongsSketch parallelSketch = new ParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize, ParallelLongsSketch.TestTypes.TEST_ZEROS, numOfInputs);
		long paralleTotalTime = parallelSketch.finishThenDispose();
		System.out.println("paralle Total Time with zeros: " + paralleTotalTime + "ms");
		
		//test new with random range = maxMapSize + 7
		parallelSketch = new ParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize, ParallelLongsSketch.TestTypes.TEST_RANDOM_RANGE, numOfInputs, smallRandomRange);
		paralleTotalTime = parallelSketch.finishThenDispose();
		System.out.println("paralle Total Time with random range of " + smallRandomRange + " numbers: " + paralleTotalTime + "ms");
		
		//test new with random range = 100000
		parallelSketch = new ParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize, ParallelLongsSketch.TestTypes.TEST_RANDOM_RANGE, numOfInputs, bigRandomRange);
		paralleTotalTime = parallelSketch.finishThenDispose();
		System.out.println("paralle Total Time with random range of 100000 numbers: " + paralleTotalTime + "ms");
	
	  	// test old with smallRandomRange
		long startoldSketchTime = System.currentTimeMillis();
		LongsSketch oldSketch = new LongsSketch(maxMapSize);
		for(long n = 0; n<numOfInputs; n++) {
			randLong = (long) (Math.random() * smallRandomRange);
			oldSketch.update(randLong);		
		}
		long oldSketchTotalTime = System.currentTimeMillis() - startoldSketchTime;
		System.out.println("old Sketch with smallRandomRange Total Time: " + oldSketchTotalTime + "ms");
		
	  	// test old with bigRandomRange
		startoldSketchTime = System.currentTimeMillis();
		oldSketch = new LongsSketch(maxMapSize);
		for(long n = 0; n<numOfInputs; n++) {
			randLong = (long) (Math.random() * bigRandomRange);
			oldSketch.update(randLong);		
		}
		oldSketchTotalTime = System.currentTimeMillis() - startoldSketchTime;
		System.out.println("old Sketch with bigRandomRange Total Time: " + oldSketchTotalTime + "ms");
		
		System.out.println("\nParallelLongsSketchSpeedTest ended");
}

  @Test
  public void basicParallelLongsSketchTest() {
	  	int numOfLocalSketches = 99;
	  	int maxMapSize = 128;
	  	int maxSketchsSize = 10000;
	  	long numOfInputs = 10000000L; 
		ParallelLongsSketch parallelSketch = new ParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize);
		LongsSketch oldSketch = new LongsSketch(maxMapSize);
		
		System.out.println("basicParallelLongsSketchTest started:");
				
		long randLong;
		for(long n = 0; n<numOfInputs; n++) {
	    	if(Math.random() < 0.75) {
	    		randLong = (long) (Math.random() * 10); 
	    	}
	    	else {
	    		randLong = (long) (Math.random() * 10000); 
	    	}
	    	parallelSketch.update(randLong);
	    	oldSketch.update(randLong);
		}
		sleep(2000);

		System.out.format("\ncalling finishThenDispose()\n");
		
		long estTime = parallelSketch.finishThenDispose();
		System.out.format("estTime = " + estTime + "\n");
		
		if (parallelSketch.getStreamLength() < oldSketch.getStreamLength()) {
			// add print of sizes
			System.out.format("\nfailed to ingest all the inputs");
			System.out.format(", StreamLength = " + parallelSketch.getStreamLength());
			
			return;
		}
		
		Row[] parallelFrequentItems = parallelSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
		Row[] oldSketchFrequentItems = oldSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
		
		System.out.format("parallel error is: " + parallelSketch.getMaximumError() + "\n");
		System.out.format("oldSketch error is: " + oldSketch.getMaximumError() + "\n\n");
		
		System.out.format(" Parallel Frequent Items:\n");
		System.out.format(Row.getRowHeader() + "\n");
		for (Row r : parallelFrequentItems) {	
			System.out.format(r.toString() + "\n");
		}
		
		System.out.format("\nOld Sketch Frequent Items:\n");
		System.out.format(Row.getRowHeader() + "\n");
		for (Row r : oldSketchFrequentItems) {	
			System.out.format(r.toString() + "\n");
		}
		
		System.out.format("done\n");
	}

  @Test
  public void basicComposableLongsSketchTest() {
	int oldSketchSize = 128;
	int globalSketchSize = 128;
	int localSketchSize = 128;
	int numOfLocalSketches = 16;
	int numOfInputsToMerge = 10000;
	long numOfInputs = 100000000L;
	println("basicComposableLongsSketchTest started");
    LongsSketch oldSketch = new LongsSketch(oldSketchSize);
    ComposableLongsSketch globalSketch = new ComposableLongsSketch(globalSketchSize);
    LongsSketch[] locals = new LongsSketch[numOfLocalSketches];
    for (int i=0; i<numOfLocalSketches; i++) {
    	locals[i] = new LongsSketch(localSketchSize);
    }
    
    int i=0, m=0;
    long randLong;   
    for(long n = 0; n<numOfInputs; n++) {
    	////System.out.format("%d: \n" ,n+1);
    	if(i >= numOfLocalSketches) {
    		i=0;
    	}
    	if(Math.random() < 0.5) {
    		randLong = (long) (Math.random() * 10); 
    	}
    	else {
    		randLong = (long) (Math.random() * 10000); 
    	}
    	////randLong = (long) (Math.random() * Integer.MAX_VALUE);
    	////randLong = 1;
    	////System.out.format("%d\t%d\n", randLong, n);
    	oldSketch.update(randLong);
    	locals[i].update(randLong);
    	m++;
    	i++;
    	if(m >= numOfLocalSketches * numOfInputsToMerge) {
    		for(int j=0; j<numOfLocalSketches; j++) {
    			globalSketch.merge(locals[j]);
    			locals[j].reset();
    			////System.out.format("", j, globalSketch.getStreamLength());
    		}
    		m=0;
    	}
    }
	for(int j=0; j<numOfLocalSketches; j++) {
		globalSketch.merge(locals[j]);
		locals[j].reset();
		////System.out.format("final size after merging local %d: %d\n", j, globalSketch.getStreamLength());
	}
	Row[] R1, R2;
	
	boolean printResults = true;
	if(printResults) {
		System.out.format("\nold Sketch:\n" + oldSketch.toString() + "\n");
		System.out.format("\nglobal Sketch:\n" + globalSketch.toString() + "\n");
		/*
		R1 = oldSketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
		System.out.format("\n oldSketch: ErrorType = NO_FALSE_NEGATIVES\n");
		System.out.format(Row.getRowHeader() + "\n");
		for (Row r : R1) {
			System.out.format(r.toString() + "\n");
		}
		
		R2 = globalSketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);	
		System.out.format("\n globalSketch: ErrorType = NO_FALSE_NEGATIVES\n");
		System.out.format(Row.getRowHeader() + "\n");
		for (Row r : R2) {
			System.out.format(r.toString() + "\n");
		}
		*/
		R1 = oldSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);	
		System.out.format("\n oldSketch: ErrorType = NO_FALSE_POSITIVES\n");
		System.out.format(Row.getRowHeader() + "\n");
		for (Row r : R1) {
			System.out.format(r.toString() + "\n");
		}
		
		R2 = globalSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);	
		System.out.format("\n globalSketch: ErrorType = NO_FALSE_POSITIVES\n");
		System.out.format(Row.getRowHeader() + "\n");
		for (Row r : R2) {
			System.out.format(r.toString() + "\n");
		}
	}
	else {
		R1 = oldSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
		R2 = globalSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
		boolean success = true;
		int size = R1.length;
		  if (size != R2.length) {
			  success = false;
		  }
		  for(int k=0; k<size; k++) {
			  if (!R1[k].equals(R2[k])) {
				  success = false;
			  }
		  }
		assert(success == true);
	}
	
	System.out.format("\nbasicComposableLongsSketchTest end\n");
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.err.println(s); //disable here
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
