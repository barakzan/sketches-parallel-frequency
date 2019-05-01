/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.isPowerOf2;
import static com.yahoo.sketches.Util.toLog2;
import static com.yahoo.sketches.frequencies.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.frequencies.PreambleUtil.SER_VER;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractActiveItems;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractFlags;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractLgCurMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractLgMaxMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.frequencies.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertActiveItems;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertFlags;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertLgCurMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertLgMaxMapSize;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.frequencies.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.frequencies.Util.LG_MIN_MAP_SIZE;
import static com.yahoo.sketches.frequencies.Util.SAMPLE_SIZE;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.ArrayList;
import java.util.Comparator;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.frequencies.LongsSketch.Row;

/**
 * <p>This sketch is useful for tracking approximate frequencies of <i>long</i> items with optional
 * associated counts (<i>long</i> item, <i>long</i> count) that are members of a multiset of
 * such items. The true frequency of an item is defined to be the sum of associated counts.</p>
 *
 * <p>This implementation provides the following capabilities:</p>
 * <ul>
 * <li>Estimate the frequency of an item.</li>
 * <li>Return upper and lower bounds of any item, such that the true frequency is always
 * between the upper and lower bounds.</li>
 * <li>Return a global maximum error that holds for all items in the stream.</li>
 * <li>Return an array of frequent items that qualify either a NO_FALSE_POSITIVES or a
 * NO_FALSE_NEGATIVES error type.</li>
 * <li>Merge itself with another sketch object created from this class.</li>
 * <li>Serialize/Deserialize to/from a String or byte array.</li>
 * </ul>
 *
 * <p><b>Space Usage</b></p>
 *
 * <p>The sketch is initialized with a <i>maxMapSize</i> that specifies the maximum physical
 * length of the internal hash map of the form (<i>long</i> item, <i>long</i> count).
 * The <i>maxMapSize</i> must be a power of 2.</p>
 *
 * <p>The hash map starts at a very small size (8 entries), and grows as needed up to the
 * specified <i>maxMapSize</i>.</p>
 *
 * <p>At any moment the internal memory space usage of this sketch is 18 * <i>mapSize</i> bytes,
 * plus a small constant number of additional bytes. The maximum internal memory space usage of
 * this sketch will never exceed 18 * <i>maxMapSize</i> bytes, plus a small constant number of
 * additional bytes.</p>
 *
 * <p><b>Maximum Capacity of the Sketch</b></p>
 *
 * <p>The LOAD_FACTOR for the hash map is internally set at 75%,
 * which means at any time the map capacity of (item, count) pairs is <i>mapCap</i> =
 * 0.75 * <i>mapSize</i>.
 * The maximum capacity of (item, count) pairs of the sketch is <i>maxMapCap</i> =
 * 0.75 * <i>maxMapSize</i>.</p>
 *
 * <p><b>Updating the sketch with (item, count) pairs</b></p>
 *
 * <p>If the item is found in the hash map, the mapped count field (the "counter") is
 * incremented by the incoming count, otherwise, a new counter "(item, count) pair" is
 * created. If the number of tracked counters reaches the maximum capacity of the hash map
 * the sketch decrements all of the counters (by an approximately computed median), and
 * removes any non-positive counters.</p>
 *
 * <p><b>Accuracy</b></p>
 *
 * <p>If fewer than 0.75 * <i>maxMapSize</i> different items are inserted into the sketch the
 * estimated frequencies returned by the sketch will be exact.</p>
 *
 * <p>The logic of the frequent items sketch is such that the stored counts and true counts are
 * never too different.
 * More specifically, for any <i>item</i>, the sketch can return an estimate of the
 * true frequency of <i>item</i>, along with upper and lower bounds on the frequency
 * (that hold deterministically).</p>
 *
 * <p>For this implementation and for a specific active <i>item</i>, it is guaranteed that
 * the true frequency will be between the Upper Bound (UB) and the Lower Bound (LB) computed for
 * that <i>item</i>.  Specifically, <i>(UB- LB) &le; W * epsilon</i>, where <i>W</i> denotes the
 * sum of all item counts, and <i>epsilon = 3.5/M</i>, where <i>M</i> is the <i>maxMapSize</i>.</p>
 *
 * <p>This is a worst case guarantee that applies to arbitrary inputs.<sup>1</sup>
 * For inputs typically seen in practice <i>(UB-LB)</i> is usually much smaller.
 * </p>
 *
 * <p><b>Background</b></p>
 *
 * <p>This code implements a variant of what is commonly known as the "Misra-Gries
 * algorithm". Variants of it were discovered and rediscovered and redesigned several times
 * over the years:</p>
 * <ul><li>"Finding repeated elements", Misra, Gries, 1982</li>
 * <li>"Frequency estimation of Internet packet streams with limited space" Demaine,
 * Lopez-Ortiz, Munro, 2002</li>
 * <li>"A simple algorithm for finding frequent elements in streams and bags" Karp, Shenker,
 * Papadimitriou, 2003</li>
 * <li>"Efficient Computation of Frequent and Top-k Elements in Data Streams" Metwally,
 * Agrawal, Abbadi, 2006</li>
 * </ul>
 *
 * <sup>1</sup> For speed we do employ some randomization that introduces a small probability that
 * our proof of the worst-case bound might not apply to a given run.  However, we have ensured
 * that this probability is extremely small. For example, if the stream causes one table purge
 * (rebuild), our proof of the worst case bound applies with probability at least 1 - 1E-14.
 * If the stream causes 1E9 purges, our proof applies with probability at least 1 - 1E-5.
 *
 * @author Justin Thaler
 * @author Lee Rhodes
 */
public class ComposableLongsSketch {
	
  private static final int STR_PREAMBLE_TOKENS = 6;
  
  /**
   * an atomic integer that represents the cell which getEstimate will read from  
   */
  public enum Cell {
	  ReadCell,
	  WriteCell;
  }
  
  private AtomicInteger CellToRead = new AtomicInteger(0);
  
  private int GetReadCellIndex() {
	  return CellToRead.get();
  }
  
  private int GetWriteCellIndex() {
	  return CellToRead.get() == 0 ? 1 : 0;
  }
  
  private void FlipCells() {
	  CellToRead.set(CellToRead.get() == 0 ? 1 : 0);
  }
  
  /**
   * Log2 Maximum length of the arrays internal to the hash map supported by the data
   * structure.
   */
  private int lgMaxMapSize;

  /**
   * The current number of counters supported by the hash map.
   */
  private int[] curMapCap = new int[2]; //the threshold to purge
  
  private int getCurMapCap() {
	  return curMapCap[GetReadCellIndex()];
  }
  
  private int getCurMapCap(Cell cellToRead) {
	  if(cellToRead == Cell.ReadCell) {
		  return curMapCap[GetReadCellIndex()];
	  }
	  else { // Cell.WriteCell
		  return curMapCap[GetWriteCellIndex()];
	  }
  }
  
  private void setCurMapCap(int value) {
	  curMapCap[0] = value;
	  curMapCap[1] = value;
  }

  private void setCurMapCap(int value, Cell cellToSet) {
	  if(cellToSet == Cell.ReadCell) {
		  curMapCap[GetReadCellIndex()] = value;
	  }
	  else { // Cell.WriteCell
		  curMapCap[GetWriteCellIndex()] = value;
	  }
  }
  
  /**
   * Tracks the total of decremented counts.
   */
  private long[] offset = new long[2];

  private long getOffset() {
	  return offset[GetReadCellIndex()];
  }
  
  private long getOffset(Cell cellToRead) {
	  if(cellToRead == Cell.ReadCell) {
		  return offset[GetReadCellIndex()];
	  }
	  else {
		  return offset[GetWriteCellIndex()];
	  }
  }
  
  private void setOffset(long toSet) {
	  offset[0] = toSet;
	  offset[1] = toSet;
  }
  
  private void setOffset(long value, Cell cellToSet) {
	  if(cellToSet == Cell.ReadCell) {
		  offset[GetReadCellIndex()] = value;
	  }
	  else {
		  offset[GetWriteCellIndex()] = value;
	  }
  }
  
  /**
   * The sum of all frequencies of the stream so far.
   */
  private long[] streamWeight = {0,0};

  private long getStreamWeight() {
	  return streamWeight[GetReadCellIndex()];
  }
  
  private long getStreamWeight(Cell cellToRead) {
	  if(cellToRead == Cell.ReadCell) {
		  return streamWeight[GetReadCellIndex()];
	  }
	  else {
		  return streamWeight[GetWriteCellIndex()];
	  }
  }
  
  private void setStreamWeight(long value) {
	  streamWeight[0] = value;
	  streamWeight[1] = value;
  }
  
  private void setStreamWeight(long value, Cell cellToSet) {
	  if(cellToSet == Cell.ReadCell) {
		  streamWeight[GetReadCellIndex()] = value;
	  }
	  else {
		  streamWeight[GetWriteCellIndex()] = value;
	  }
  }
  
  /**
   * The maximum number of samples used to compute approximate median of counters when doing
   * decrement
   */
  private int sampleSize;

  /**
   * Hash map mapping stored items to approximate counts
   */
  private ReversePurgeLongHashMap hashMap[] = new ReversePurgeLongHashMap[2];

  private ReversePurgeLongHashMap getHashMap() {
	  return hashMap[GetReadCellIndex()];
  }
  
  private ReversePurgeLongHashMap getHashMap(Cell cellToRead) {
	  if(cellToRead == Cell.ReadCell) {
		  return hashMap[GetReadCellIndex()];
	  }
	  else {
		  return hashMap[GetWriteCellIndex()];
	  }
  }
  
  private void setHashMap(ReversePurgeLongHashMap mapToSet, Cell cellToSet) {
	  if(cellToSet == Cell.ReadCell) {
		  hashMap[GetReadCellIndex()] = mapToSet;
	  }
	  else {
		  hashMap[GetWriteCellIndex()] = mapToSet;
	  }
  }
  
  /**
   * Construct this sketch with the parameter maxMapSize and the default initialMapSize (8).
   *
   * @param maxMapSize Determines the physical size of the internal hash map managed by this
   * sketch and must be a power of 2.  The maximum capacity of this internal hash map is
   * 0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are a
   * function of maxMapSize.
   */
  public ComposableLongsSketch(final int maxMapSize) {
    this(toLog2(maxMapSize, "maxMapSize"), LG_MIN_MAP_SIZE);
  }

  /**
   * Construct this sketch with parameter lgMapMapSize and lgCurMapSize. This internal
   * constructor is used when deserializing the sketch.
   *
   * @param lgMaxMapSize Log2 of the physical size of the internal hash map managed by this
   * sketch. The maximum capacity of this internal hash map is 0.75 times 2^lgMaxMapSize.
   * Both the ultimate accuracy and size of this sketch are a function of lgMaxMapSize.
   *
   * @param lgCurMapSize Log2 of the starting (current) physical size of the internal hash
   * map managed by this sketch.
   */
  ComposableLongsSketch(final int lgMaxMapSize, final int lgCurMapSize) {
    //set initial size of hash map
    this.lgMaxMapSize = Math.max(lgMaxMapSize, LG_MIN_MAP_SIZE);
    final int lgCurMapSz = Math.max(lgCurMapSize, LG_MIN_MAP_SIZE);
    hashMap[0] = new ReversePurgeLongHashMap(1 << lgCurMapSz);
    hashMap[1] = new ReversePurgeLongHashMap(1 << lgCurMapSz);
    setCurMapCap(getHashMap().getCapacity());
    curMapCap[1] = getHashMap().getCapacity();
    final int maxMapCap =
        (int) ((1 << lgMaxMapSize) * ReversePurgeLongHashMap.getLoadFactor());
    setOffset(0);
    sampleSize = Math.min(SAMPLE_SIZE, maxMapCap);
  }

  /**
   * Returns a sketch instance of this class from the given srcMem,
   * which must be a Memory representation of this sketch class.
   *
   * @param srcMem a Memory representation of a sketch of this class.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a sketch instance of this class.
   */
  public static ComposableLongsSketch getInstance(final Memory srcMem) {
    final long pre0 = PreambleUtil.checkPreambleSize(srcMem); //make sure preamble will fit
    final int maxPreLongs = Family.FREQUENCY.getMaxPreLongs();

    final int preLongs = extractPreLongs(pre0);         //Byte 0
    final int serVer = extractSerVer(pre0);             //Byte 1
    final int familyID = extractFamilyID(pre0);         //Byte 2
    final int lgMaxMapSize = extractLgMaxMapSize(pre0); //Byte 3
    final int lgCurMapSize = extractLgCurMapSize(pre0); //Byte 4
    final boolean empty = (extractFlags(pre0) & EMPTY_FLAG_MASK) != 0; //Byte 5

    // Checks
    final boolean preLongsEq1 = (preLongs == 1);        //Byte 0
    final boolean preLongsEqMax = (preLongs == maxPreLongs);
    if (!preLongsEq1 && !preLongsEqMax) {
      throw new SketchesArgumentException(
          "Possible Corruption: PreLongs must be 1 or " + maxPreLongs + ": " + preLongs);
    }
    if (serVer != SER_VER) {                            //Byte 1
      throw new SketchesArgumentException(
          "Possible Corruption: Ser Ver must be " + SER_VER + ": " + serVer);
    }
    final int actFamID = Family.FREQUENCY.getID();      //Byte 2
    if (familyID != actFamID) {
      throw new SketchesArgumentException(
          "Possible Corruption: FamilyID must be " + actFamID + ": " + familyID);
    }
    if (empty ^ preLongsEq1) {                          //Byte 5 and Byte 0
      throw new SketchesArgumentException(
          "Possible Corruption: (PreLongs == 1) ^ Empty == True.");
    }

    if (empty) {
      return new ComposableLongsSketch(lgMaxMapSize, LG_MIN_MAP_SIZE);
    }
    //get full preamble
    final long[] preArr = new long[preLongs];
    srcMem.getLongArray(0, preArr, 0, preLongs);

    final ComposableLongsSketch fls = new ComposableLongsSketch(lgMaxMapSize, lgCurMapSize);
    fls.setStreamWeight(0); //update after
    fls.setOffset(preArr[3]);

    final int preBytes = preLongs << 3;
    final int activeItems = extractActiveItems(preArr[1]);
    //Get countArray
    final long[] countArray = new long[activeItems];
    srcMem.getLongArray(preBytes, countArray, 0, activeItems);
    //Get itemArray
    final int itemsOffset = preBytes + (8 * activeItems);
    final long[] itemArray = new long[activeItems];
    srcMem.getLongArray(itemsOffset, itemArray, 0, activeItems);
    //update the sketch
    for (int i = 0; i < activeItems; i++) {
      fls.update(itemArray[i], countArray[i]);
    }
    fls.setStreamWeight(preArr[2]); //override streamWeight due to updating
    return fls;
  }

  /**
   * Returns a sketch instance of this class from the given String,
   * which must be a String representation of this sketch class.
   *
   * @param string a String representation of a sketch of this class.
   * @return a sketch instance of this class.
   */
  public static ComposableLongsSketch getInstance(final String string) {
    final String[] tokens = string.split(",");
    if (tokens.length < (STR_PREAMBLE_TOKENS + 2)) {
      throw new SketchesArgumentException(
          "String not long enough: " + tokens.length);
    }
    final int serVer  = Integer.parseInt(tokens[0]);
    final int famID   = Integer.parseInt(tokens[1]);
    final int lgMax   = Integer.parseInt(tokens[2]);
    final int flags   = Integer.parseInt(tokens[3]);
    final long streamWt = Long.parseLong(tokens[4]);
    final long offset       = Long.parseLong(tokens[5]); //error offset
    //should always get at least the next 2 from the map
    final int numActive = Integer.parseInt(tokens[6]);
    final int lgCur = Integer.numberOfTrailingZeros(Integer.parseInt(tokens[7]));

    //checks
    if (serVer != SER_VER) {
      throw new SketchesArgumentException("Possible Corruption: Bad SerVer: " + serVer);
    }
    Family.FREQUENCY.checkFamilyID(famID);
    final boolean empty = flags > 0;
    if (!empty && (numActive == 0)) {
      throw new SketchesArgumentException(
          "Possible Corruption: !Empty && NumActive=0;  strLen: " + numActive);
    }
    final int numTokens = tokens.length;
    if ((2 * numActive) != (numTokens - STR_PREAMBLE_TOKENS - 2)) {
      throw new SketchesArgumentException(
          "Possible Corruption: Incorrect # of tokens: " + numTokens
            + ", numActive: " + numActive);
    }

    final ComposableLongsSketch sketch = new ComposableLongsSketch(lgMax, lgCur);
    sketch.setStreamWeight(streamWt);
    sketch.setOffset(offset);
    sketch.hashMap[0] = LongsSketch.deserializeFromStringArray(tokens);
    sketch.hashMap[1] = LongsSketch.deserializeFromStringArray(tokens);
    return sketch;
  }

  /**
   * Returns the estimated <i>a priori</i> error given the maxMapSize for the sketch and the
   * estimatedTotalStreamWeight.
   * @param maxMapSize the planned map size to be used when constructing this sketch.
   * @param estimatedTotalStreamWeight the estimated total stream weight.
   * @return the estimated <i>a priori</i> error.
   */
  public static double getAprioriError(final int maxMapSize, final long estimatedTotalStreamWeight) {
    return getEpsilon(maxMapSize) * estimatedTotalStreamWeight;
  }

  /**
   * Returns the current number of counters the sketch is configured to support.
   *
   * @return the current number of counters the sketch is configured to support.
   */
  public int getCurrentMapCapacity() {
    return getCurMapCap();
  }

  /**
   * Returns epsilon used to compute <i>a priori</i> error.
   * This is just the value <i>3.5 / maxMapSize</i>.
   * @param maxMapSize the planned map size to be used when constructing this sketch.
   * @return epsilon used to compute <i>a priori</i> error.
   */
  public static double getEpsilon(final int maxMapSize) {
    if (!isPowerOf2(maxMapSize)) {
      throw new SketchesArgumentException("maxMapSize is not a power of 2.");
    }
    return 3.5 / maxMapSize;
  }

  /**
   * Gets the estimate of the frequency of the given item.
   * Note: The true frequency of a item would be the sum of the counts as a result of the
   * two update functions.
   *
   * @param item the given item
   * @return the estimate of the frequency of the given item
   */
  public long getEstimate(final long item) {
    // If item is tracked:
    // Estimate = itemCount + offset; Otherwise it is 0.
    final long itemCount = getHashMap().get(item);
    return (itemCount > 0) ? itemCount + getOffset() : 0;
  }

  /**
   * Gets the guaranteed lower bound frequency of the given item, which can never be
   * negative.
   *
   * @param item the given item.
   * @return the guaranteed lower bound frequency of the given item. That is, a number which
   * is guaranteed to be no larger than the real frequency.
   */
  public long getLowerBound(final long item) {
    //LB = itemCount or 0
    return getHashMap().get(item);
  }

  /**
   * Returns an array of Rows that include frequent items, estimates, upper and lower bounds
   * given a threshold and an ErrorCondition. If the threshold is lower than getMaximumError(),
   * then getMaximumError() will be used instead.
   *
   * <p>The method first examines all active items in the sketch (items that have a counter).
   *
   * <p>If <i>ErrorType = NO_FALSE_NEGATIVES</i>, this will include an item in the result
   * list if getUpperBound(item) &gt; threshold.
   * There will be no false negatives, i.e., no Type II error.
   * There may be items in the set with true frequencies less than the threshold
   * (false positives).</p>
   *
   * <p>If <i>ErrorType = NO_FALSE_POSITIVES</i>, this will include an item in the result
   * list if getLowerBound(item) &gt; threshold.
   * There will be no false positives, i.e., no Type I error.
   * There may be items omitted from the set with true frequencies greater than the
   * threshold (false negatives). This is a subset of the NO_FALSE_NEGATIVES case.</p>
   *
   * @param threshold to include items in the result list
   * @param errorType determines whether no false positives or no false negatives are
   * desired.
   * @return an array of frequent items
   */
  public LongsSketch.Row[] getFrequentItems(final long threshold, final ErrorType errorType) {
    return getSortItems(threshold > getMaximumError() ? threshold : getMaximumError(), errorType);
  }

  /**
   * Returns an array of Rows that include frequent items, estimates, upper and lower bounds
   * given an ErrorCondition and the default threshold.
   * This is the same as getFrequentItems(getMaximumError(), errorType)
   *
   * @param errorType determines whether no false positives or no false negatives are
   * desired.
   * @return an array of frequent items
   */
  public LongsSketch.Row[] getFrequentItems(final ErrorType errorType) {
    return getSortItems(getMaximumError(), errorType);
  }

  private LongsSketch.Row[] getSortItems(final long threshold, final ErrorType errorType) {
	  LongsSketch.Row[] R1 = sortItems(threshold, errorType);
	  LongsSketch.Row[] R2 = sortItems(threshold, errorType);
	  while(!RowArraysAreEqual(R1, R2)) {
		  R1 = R2;
		  R2 = sortItems(threshold, errorType);
	  }
	  return R1;
  }
  
  /**
   * @return An upper bound on the maximum error of getEstimate(item) for any item.
   * This is equivalent to the maximum distance between the upper bound and the lower bound
   * for any item.
   */
  public long getMaximumError() {
    return getOffset();
  }

  /**
   * Returns the maximum number of counters the sketch is configured to support.
   *
   * @return the maximum number of counters the sketch is configured to support.
   */
  public int getMaximumMapCapacity() {
    return (int) ((1 << lgMaxMapSize) * ReversePurgeLongHashMap.getLoadFactor());
  }

  /**
   * @return the number of active items in the sketch.
   */
  public int getNumActiveItems() {
    return getHashMap().getNumActive();
  }
  
  public int getNumActiveItems(Cell cellToRead) {
	    return getHashMap(cellToRead).getNumActive();
	  }

  /**
   * Returns the number of bytes required to store this sketch as an array of bytes.
   *
   * @return the number of bytes required to store this sketch as an array of bytes.
   */
  public int getStorageBytes() {
    if (isEmpty()) { return 8; }
    return (4 * 8) + (16 * getNumActiveItems());
  }

  /**
   * Returns the sum of the frequencies (weights or counts) in the stream seen so far by the sketch
   *
   * @return the sum of the frequencies in the stream seen so far by the sketch
   */
  public long getStreamLength() {
    return getStreamWeight();
  }

  /**
   * Gets the guaranteed upper bound frequency of the given item.
   *
   * @param item the given item
   * @return the guaranteed upper bound frequency of the given item. That is, a number which
   * is guaranteed to be no smaller than the real frequency.
   */
  public long getUpperBound(final long item) {
    // UB = itemCount + offset
    return getHashMap().get(item) + getOffset();
  }

  /**
   * Returns true if this sketch is empty
   *
   * @return true if this sketch is empty
   */
  public boolean isEmpty() {
    return getNumActiveItems() == 0;
  }

  /**
   * This function merges the other Composable sketch into this one.
   * The other Composable sketch may be of a different size.
   *
   * @param other Composable sketch of this class
   * @return a Composable sketch whose estimates are within the guarantees of the
   * largest error tolerance of the two merged Composable sketches.
   */
  public ComposableLongsSketch merge(final ComposableLongsSketch other) {
    if (other == null) { return this; }
    if (other.getStreamLength() == 0) { return this; }

    final long streamWt = getStreamWeight() + other.getStreamWeight(); //capture before merge

    final ReversePurgeLongHashMap.Iterator iter = other.getHashMap().iterator();
    while (iter.next()) { //this may add to offset during rebuilds
      this.update(iter.getKey(), iter.getValue());
    }
    setOffset(getOffset() + other.getOffset());
    setStreamWeight(streamWt); //corrected streamWeight
    return this;
  }

  /**
   * This function merges the other sketch into this Composable sketch.
   * The other sketch may be of a different size.
   *
   * @param other sketch of this class
   * @return a Composable sketch whose estimates are within the guarantees of the
   * largest error tolerance of the two merged sketches.
   */
  public ComposableLongsSketch merge(final LongsSketch other) {
    if (other == null) { return this; }
    if (other.getStreamLength() == 0) { return this; }

    assert(getStreamWeight(Cell.WriteCell) == getStreamWeight(Cell.ReadCell));
    final long streamWt = getStreamWeight(Cell.WriteCell) + other.getStreamWeight(); //capture before merge

    final ReversePurgeLongHashMap.Iterator iter = other.getHashMap().iterator();
    while (iter.next()) { //this may add to offset during rebuilds
      this.update(iter.getKey(), iter.getValue());
    }
    setOffset(getOffset(Cell.WriteCell) + other.getOffset(), Cell.WriteCell);
    setStreamWeight(streamWt, Cell.WriteCell); //corrected streamWeight
    
    switchCellsAndCopyProperties();
    return this;
  }
  
  /**
   * 
   */
  private void switchCellsAndCopyProperties() {
	  FlipCells();
	  setCurMapCap(getCurMapCap(),Cell.WriteCell);
	  setOffset(getOffset(), Cell.WriteCell);
	  setStreamWeight(getStreamWeight(), Cell.WriteCell);
	  setHashMap(ReversePurgeLongHashMap.getInstance(getHashMap().serializeToString()), Cell.WriteCell);
  }
  
  /**
   * Resets this sketch to a virgin state.
   */
  public void reset() {
	  //TODO: get last table size and reset to it
    hashMap[0] = new ReversePurgeLongHashMap(1 << LG_MIN_MAP_SIZE);
    hashMap[1] = new ReversePurgeLongHashMap(1 << LG_MIN_MAP_SIZE);
    setCurMapCap(getHashMap().getCapacity());
    setOffset(0);
    setStreamWeight(0);
  }

  //Serialization

  /**
   * Returns a String representation of this sketch
   *
   * @return a String representation of this sketch
   */
  public String serializeToString() {
	  // TODO: getHashMap() is not good when bit is changed, maybe add lock
    final StringBuilder sb = new StringBuilder();
    //start the string with parameters of the sketch
    final int serVer = SER_VER;                 //0
    final int famID = Family.FREQUENCY.getID(); //1
    final int lgMaxMapSz = lgMaxMapSize;        //2
    final int flags = (getHashMap().getNumActive() == 0) ? EMPTY_FLAG_MASK : 0; //3
    final String fmt = "%d,%d,%d,%d,%d,%d,";
    final String s =
        String.format(fmt, serVer, famID, lgMaxMapSz, flags, streamWeight[0], offset[0]);
    sb.append(s);
    sb.append(getHashMap().serializeToString()); //numActive, curMaplen, key[i], value[i], ...
    // maxMapCap, samplesize are deterministic functions of maxMapSize,
    //  so we don't need them in the serialization
    return sb.toString();
  }

  /**
   * Returns a byte array representation of this sketch
   * @return a byte array representation of this sketch
   */
  public byte[] toByteArray() {
	// TODO: getHashMap() is not good when bit is changed, maybe add lock
    final int preLongs, outBytes;
    final boolean empty = isEmpty();
    final int activeItems = getNumActiveItems();
    if (empty) {
      preLongs = 1;
      outBytes = 8;
    } else {
      preLongs = Family.FREQUENCY.getMaxPreLongs(); //4
      outBytes = (preLongs + (2 * activeItems)) << 3; //2 because both keys and values are longs
    }
    final byte[] outArr = new byte[outBytes];
    final WritableMemory mem = WritableMemory.wrap(outArr);

    // build first preLong empty or not
    long pre0 = 0L;
    pre0 = insertPreLongs(preLongs, pre0);                  //Byte 0
    pre0 = insertSerVer(SER_VER, pre0);                     //Byte 1
    pre0 = insertFamilyID(Family.FREQUENCY.getID(), pre0);  //Byte 2
    pre0 = insertLgMaxMapSize(lgMaxMapSize, pre0);          //Byte 3
    pre0 = insertLgCurMapSize(getHashMap().getLgLength(), pre0); //Byte 4
    pre0 = (empty) ? insertFlags(EMPTY_FLAG_MASK, pre0) : insertFlags(0, pre0); //Byte 5

    if (empty) {
      mem.putLong(0, pre0);
    } else {
      final long pre = 0;
      final long[] preArr = new long[preLongs];
      preArr[0] = pre0;
      preArr[1] = insertActiveItems(activeItems, pre);
      preArr[2] = getStreamWeight();
      preArr[3] = getOffset();
      mem.putLongArray(0, preArr, 0, preLongs);
      final int preBytes = preLongs << 3;
      mem.putLongArray(preBytes, getHashMap().getActiveValues(), 0, activeItems);

      mem.putLongArray(preBytes + (activeItems << 3), getHashMap().getActiveKeys(), 0,
          activeItems);
    }
    return outArr;
  }

  /**
   * Returns a human readable summary of this sketch.
   * @return a human readable summary of this sketch.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("FrequentLongsSketch:").append(LS);
    sb.append("  Stream Length    : " + streamWeight[0]).append(LS);
    sb.append("  Max Error Offset : " + offset[0]).append(LS);
    sb.append(getHashMap().toString());
    return sb.toString();
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of a LongsSketch.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of a LongsSketch.
   */
  public static String toString(final byte[] byteArr) {
    return toString(Memory.wrap(byteArr));
  }

  /**
   * Returns a human readable string of the preamble of a Memory image of a LongsSketch.
   * @param mem the given Memory object
   * @return  a human readable string of the preamble of a Memory image of a LongsSketch.
   */
  public static String toString(final Memory mem) {
    return PreambleUtil.preambleToString(mem);
  }

  /**
   * Update this sketch with an item and a frequency count of one.
   * @param item for which the frequency should be increased.
 
  private void update(final long item) {
    update(item, 1);
  }  */

  /**
   * Update this sketch with a item and a positive frequency count (or weight).
   * @param item for which the frequency should be increased. The item can be any long value
   * and is only used by the sketch to determine uniqueness.
   * @param count the amount by which the frequency of the item should be increased.
   * An count of zero is a no-op, and a negative count will throw an exception.
   */
  private void update(final long item, final long count) {
    if (count == 0) { return; }
    if (count < 0) {
      throw new SketchesArgumentException("Count may not be negative");
    }
    setStreamWeight(getStreamWeight(Cell.WriteCell) + count, Cell.WriteCell);
    getHashMap(Cell.WriteCell).adjustOrPutValue(item, count);

    if (getNumActiveItems(Cell.WriteCell) > getCurMapCap(Cell.WriteCell)) { //over the threshold, we need to do something
      if (getHashMap(Cell.WriteCell).getLgLength() < lgMaxMapSize) { //below tgt size, we can grow
    	  getHashMap(Cell.WriteCell).resize(2 * getHashMap(Cell.WriteCell).getLength());
        setCurMapCap(getHashMap(Cell.WriteCell).getCapacity(), Cell.WriteCell);
      } else { //At tgt size, must purge
    	setOffset(getOffset(Cell.WriteCell) + getHashMap(Cell.WriteCell).purge(sampleSize), Cell.WriteCell);
        if (getNumActiveItems(Cell.WriteCell) > getMaximumMapCapacity()) {
          throw new SketchesStateException("Purge did not reduce active items.");
        }
      }
    }
  }
  
  private boolean RowArraysAreEqual(LongsSketch.Row[] R1, LongsSketch.Row[] R2) {
	  int size = R1.length;
	  if (size != R2.length) {
		  return false;
	  }
	  for(int i=0; i<size; i++) {
		  if (!R1[i].equals(R2[i])) {
			  return false;
		  }
	  }
	  return true;
  }
  
  LongsSketch.Row[] sortItems(final long threshold, final ErrorType errorType) {
	    final ArrayList<LongsSketch.Row> rowList = new ArrayList<>();
	    final ReversePurgeLongHashMap.Iterator iter = getHashMap().iterator();
	    if (errorType == ErrorType.NO_FALSE_NEGATIVES) {
	      while (iter.next()) {
	        final long est = getEstimate(iter.getKey());
	        final long ub = getUpperBound(iter.getKey());
	        final long lb = getLowerBound(iter.getKey());
	        if (ub >= threshold) {
	          final LongsSketch.Row row = new Row(iter.getKey(), est, ub, lb);
	          rowList.add(row);
	        }
	      }
	    } else { //NO_FALSE_POSITIVES
	      while (iter.next()) {
	        final long est = getEstimate(iter.getKey());
	        final long ub = getUpperBound(iter.getKey());
	        final long lb = getLowerBound(iter.getKey());
	        if (lb >= threshold) {
	          final LongsSketch.Row row = new Row(iter.getKey(), est, ub, lb);
	          rowList.add(row);
	        }
	      }
	    }

	    // descending order
	    rowList.sort(new Comparator<LongsSketch.Row>() {
	      @Override
	      public int compare(final LongsSketch.Row r1, final LongsSketch.Row r2) {
	        return r2.compareTo(r1);
	      }
	    });

	    final LongsSketch.Row[] rowsArr = rowList.toArray(new LongsSketch.Row[rowList.size()]);
	    return rowsArr;
	  }
}
