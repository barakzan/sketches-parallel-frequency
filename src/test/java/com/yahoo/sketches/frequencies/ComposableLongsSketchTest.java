/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.Util.LS;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.frequencies.LongsSketch.Row;

public class ComposableLongsSketchTest {
	
public static void main(String[] args) {
	println("Main started");
	ComposableLongsSketchTest t = new ComposableLongsSketchTest();
	  t.basicComposableLongsSketchTest();
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
    	if(Math.random() > 0.75) {
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

  //Restricted methods

  public void printSketch(int size, long[] freqArr) {
    LongsSketch fls = new LongsSketch(size);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i<freqArr.length; i++) {
      fls.update(i+1, freqArr[i]);
    }
    sb.append("Sketch Size: "+size).append(LS);
    String s = fls.toString();
    sb.append(s);
    println(sb.toString());
    printRows(fls, ErrorType.NO_FALSE_NEGATIVES);
    println("");
    printRows(fls, ErrorType.NO_FALSE_POSITIVES);
    println("");
  }

  private static void printRows(LongsSketch fls, ErrorType eType) {
    Row[] rows = fls.getFrequentItems(eType);
    String s1 = eType.toString();
    println(s1);
    String hdr = Row.getRowHeader();
    println(hdr);
    for (int i=0; i<rows.length; i++) {
      Row row = rows[i];
      String s2 = row.toString();
      println(s2);
    }
    if (rows.length > 0) { //check equals null case
      Row nullRow = null;
      assertFalse(rows[0].equals(nullRow));
    }
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.err.println(s); //disable here
  }

}
