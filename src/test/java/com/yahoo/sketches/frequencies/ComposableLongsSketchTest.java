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
	  	int numOfLocalSketches = 29;
	  	int maxMapSize = 64;
	  	int maxSketchsSize = 25000;
	  	long numOfInputs = 10000000L; 
	  	long randLong;
	  	
	  	System.out.println("ParallelLongsSketchSpeedTest started");
	  		
		//test new
	  	long startParalleTime = System.currentTimeMillis();
		ParallelLongsSketch parallelSketch = new ParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize, numOfInputs);
		parallelSketch.mergeLoacls();
		System.out.println("stream len = " + parallelSketch.getStreamLength());
		long paralleTotalTime = System.currentTimeMillis() - startParalleTime;
		System.out.println("   paralle Total Time: " + paralleTotalTime);
		
	  	// test old
		long startoldSketchTime = System.currentTimeMillis();
		LongsSketch oldSketch = new LongsSketch(maxMapSize);
		for(long n = 0; n<numOfInputs; n++) {
			randLong = (long) (Math.random() * 10000000);
			oldSketch.update(randLong);		
		}
		long oldSketchTotalTime = System.currentTimeMillis() - startoldSketchTime;
		System.out.println("old Sketch Total Time: " + oldSketchTotalTime);
		
		System.out.println("ParallelLongsSketchSpeedTest ended");
}

  @Test
  public void basicParallelLongsSketchTest() {
	  	int numOfLocalSketches = 7;
	  	int maxMapSize = 128;
	  	int maxSketchsSize = 1000;
	  	long numOfInputs = 10000000L; 
		ParallelLongsSketch parallelSketch = new ParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize, numOfInputs);
		LongsSketch oldSketch = new LongsSketch(maxMapSize);
		
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

		parallelSketch.mergeLoacls();
		
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

}
