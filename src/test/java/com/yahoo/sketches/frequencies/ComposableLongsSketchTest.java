/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.testng.annotations.Test;
import com.yahoo.sketches.frequencies.LongsSketch.Row;

public class ComposableLongsSketchTest {
	
public static void main(String[] args) {
	println("Main started");
	ComposableLongsSketchTest t1 = new ComposableLongsSketchTest();
	//t1.basicParallelLongsSketchTest();
	//t1.ParallelLongsSketchSpeedTest();
	
	t1.TotalSpeedTest();
	
	////ComposableLongsSketchTest t = new ComposableLongsSketchTest();
	////t.basicComposableLongsSketchTest();
}

	@Test
	public void TotalSpeedTest() {
		long numOfInputs = 100000000L;	
		
		int maxMapSize = 128;
		int maxSketchsSize = 10000000;
		int[] numOfLocalSketches = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 40, 50, 60, 70, 80};
		
		long[] zerosResult = new long[numOfLocalSketches.length];
		long[] sequentialResult = new long[numOfLocalSketches.length];
				
		try {
			String csvName = "results";
			FileWriter csvWriter = new FileWriter(csvName + ".csv");
			csvWriter.append("size:,oldSketch,");
			System.out.print("size:,oldSketch,");
			for (int numLocals : numOfLocalSketches) {
				csvWriter.append(numLocals + ",");
				System.out.print(numLocals + ",");
			}
			csvWriter.append("\nzeros[ms]:,");
			System.out.print("\nzeros[ms]:,");
			
			LongsSketch oldSketch = new LongsSketch(maxMapSize);
			long startoldSketchTime = System.currentTimeMillis();
			for(long n = 0; n<numOfInputs; n++) {
				oldSketch.update(0);		
			}
			long oldSketchTotalTime = System.currentTimeMillis() - startoldSketchTime;
			csvWriter.append(Long.toString(oldSketchTotalTime) + ",");
			System.out.print(Long.toString(oldSketchTotalTime) + ",");
			
			ParallelLongsSketch parallelSketch;
			
			int i = 0;
			for (int numLocals : numOfLocalSketches) {
				parallelSketch = new ParallelLongsSketch(numLocals, maxMapSize, maxSketchsSize, ParallelLongsSketch.TestTypes.TEST_ZEROS, false, numOfInputs);
				zerosResult[i] = parallelSketch.finishThenDispose();
				csvWriter.append(zerosResult[i] + ",");
				System.out.print(zerosResult[i] + ",");
				i++;
			}
			
			csvWriter.append("\nsequ[ms]:,");
			System.out.print("\nsequ[ms]:,");
			
			oldSketch = new LongsSketch(maxMapSize);
			startoldSketchTime = System.currentTimeMillis();
			for(long n = 0; n<numOfInputs; n++) {
				oldSketch.update(0);		
			}
			oldSketchTotalTime = System.currentTimeMillis() - startoldSketchTime;
			csvWriter.append(Long.toString(oldSketchTotalTime) + ",");
			System.out.print(Long.toString(oldSketchTotalTime) + ",");
				
			i = 0;
			for (int numLocals : numOfLocalSketches) {
				parallelSketch = new ParallelLongsSketch(numLocals, maxMapSize, maxSketchsSize, ParallelLongsSketch.TestTypes.TEST_SEQUENTIAL_NUMBERS, false, numOfInputs);
				sequentialResult[i] = parallelSketch.finishThenDispose();
				csvWriter.append(sequentialResult[i] + ",");
				System.out.print(sequentialResult[i] + ",");
				i++;
			}
			
			csvWriter.flush();
			csvWriter.close();
			
			String currentDir = System.getProperty("user.dir");
	        System.out.println("\nCSV location: " + currentDir + "\\" + csvName + ".csv");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

  @Test
  public void ParallelLongsSketchSpeedTest() {
	  	int numOfLocalSketches = 16;
	  	int maxMapSize = 256;
	  	int maxSketchsSize = 5000000;
	  	long numOfInputs = 1000000000L; 
	  	
	  	System.out.println("ParallelLongsSketchSpeedTest started\n");
	  	
	  	System.out.println("numOfLocalSketches - " + numOfLocalSketches);
	  	System.out.println("maxMapSize - " + maxMapSize);
	  	System.out.println("maxSketchsSize - " + maxSketchsSize);
	  	System.out.println("numOfInputs - " + numOfInputs);
	  	System.out.println();	
	  	
	  	System.out.println("___ZEROS___");
		
	  	// test old with zeros
		long startoldSketchTime = System.currentTimeMillis();
		LongsSketch oldSketch = new LongsSketch(maxMapSize);
		for(long n = 0; n<numOfInputs; n++) {
			oldSketch.update(0);		
		}
		long oldSketchTotalTime = System.currentTimeMillis() - startoldSketchTime;
		System.out.println("old Sketch Time: " + oldSketchTotalTime + "ms");
		
		//test new with zeros
		ParallelLongsSketch parallelSketch = new ParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize, ParallelLongsSketch.TestTypes.TEST_ZEROS, false, numOfInputs);
		long paralleTotalTime = parallelSketch.finishThenDispose();
		System.out.println("   paralle Time: " + paralleTotalTime + "ms");
		
		System.out.println("\n___SEQUENTIAL_NUMBERS___");
		
	  	// test old with SEQUENTIAL NUMBERS
		startoldSketchTime = System.currentTimeMillis();
		oldSketch = new LongsSketch(maxMapSize);
		for(long n = 0; n<numOfInputs; n++) {
			oldSketch.update(n);		
		}
		oldSketchTotalTime = System.currentTimeMillis() - startoldSketchTime;
		System.out.println("old Sketch Time: " + oldSketchTotalTime + "ms");
		
		//test new with SEQUENTIAL_NUMBERS
		parallelSketch = new ParallelLongsSketch(numOfLocalSketches, maxMapSize, maxSketchsSize, ParallelLongsSketch.TestTypes.TEST_SEQUENTIAL_NUMBERS, false, numOfInputs);
		paralleTotalTime = parallelSketch.finishThenDispose();
		System.out.println("   paralle Time: " + paralleTotalTime + "ms");
		
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
  
  static void print(String s) {
	  System.out.println(s); 
  }
  
  static void printTest(int toatlTests,int testNumber, int mapSize) {
	  print(time() + " : Test " + testNumber + " of " + toatlTests + "- oldSketch");
	  print("\tmapSize = " + mapSize);
  }
  
  static void printTest(int toatlTests, int testNumber, int mapSize, int numLocals, int localsSize) {
	  print(time() + " : Test " + testNumber + " of " + toatlTests + "- parallelSketch");
	  print("\tmapSize = " + mapSize + ", numLocals = " + numLocals + ", localsSize = " + localsSize);
  }
  
  static String time() {
	    DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
	    //get current date time with Date()
	    Date date = new Date();
	    return dateFormat.format(date);
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
