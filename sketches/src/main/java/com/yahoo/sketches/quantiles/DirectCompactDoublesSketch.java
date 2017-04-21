/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.K_SHORT;
import static com.yahoo.sketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.MIN_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.N_LONG;
import static com.yahoo.sketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractN;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertN;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.quantiles.Util.computeBaseBufferItems;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;
import static com.yahoo.sketches.quantiles.Util.computeRetainedItems;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryUtil;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Implements the DoublesSketch off-heap.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 * @author Jon Malkin
 */
final class DirectCompactDoublesSketch extends CompactDoublesSketch {
  private static final int MIN_DIRECT_DOUBLES_SER_VER = 3;
  private Memory mem_;

  //**CONSTRUCTORS**********************************************************
  private DirectCompactDoublesSketch(final int k) {
    super(k); //Checks k
  }

  /**
   * Converts the given UpdateDoublesSketch to this compact form.
   *
   * @param sketch the sketch to convert
   */
  static DirectCompactDoublesSketch createFromUpdateSketch(final UpdateDoublesSketch sketch,
                                                           final Memory dstMem) {
    final long memCap = dstMem.getCapacity();
    final int k = sketch.getK();
    final long n = sketch.getN();
    checkDirectMemCapacity(k, n, memCap);

    final Object memObj = dstMem.array();
    final long memAdd = dstMem.getCumulativeOffset(0L);

    //initialize dstMem
    dstMem.putLong(0, 0L); //clear pre0
    insertPreLongs(memObj, memAdd, 2);
    insertSerVer(memObj, memAdd, DoublesSketch.DOUBLES_SER_VER);
    insertFamilyID(memObj, memAdd, Family.QUANTILES.getID());
    insertK(memObj, memAdd, k);

    final int flags = COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK; // true for all compact sketches

    if (sketch.isEmpty()) {
      insertFlags(memObj, memAdd, flags | EMPTY_FLAG_MASK);
    } else {
      insertFlags(memObj, memAdd, flags);
      insertN(memObj, memAdd, n);
      insertMinDouble(memObj, memAdd, sketch.getMinValue());
      insertMaxDouble(memObj, memAdd, sketch.getMaxValue());

      final int bbCount = computeBaseBufferItems(k, n);

      final DoublesSketchAccessor inputAccessor = DoublesSketchAccessor.wrap(sketch);
      assert bbCount == inputAccessor.numItems();

      long dstMemOffset = COMBINED_BUFFER;

      // copy and sort base buffer
      final double[] bbArray = inputAccessor.getArray(0, bbCount);
      Arrays.sort(bbArray);
      dstMem.putDoubleArray(dstMemOffset, bbArray, 0, bbCount);
      dstMemOffset += bbCount << 3;

      long bitPattern = computeBitPattern(k, n);
      for (int lvl = 0; bitPattern > 0; ++lvl, bitPattern >>>= 1) {
        if ((bitPattern & 1L) > 0L) {
          inputAccessor.setLevel(lvl);
          dstMem.putDoubleArray(dstMemOffset, inputAccessor.getArray(0, k), 0, k);
          dstMemOffset += k << 3;
        }
      }
    }

    final DirectCompactDoublesSketch dcds = new DirectCompactDoublesSketch(k);
    dcds.mem_ = dstMem;

    return dcds;
  }

  /**
   * Wrap this sketch around the given compact Memory image of a DoublesSketch.
   *
   * @param srcMem the given compact Memory image of a DoublesSketch that may have data,
   * @return a sketch that wraps the given srcMem
   */
  static DirectCompactDoublesSketch wrapInstance(final Memory srcMem) {
    final long memCap = srcMem.getCapacity();

    final int preLongs;
    final int serVer;
    final int familyID;
    final int flags;
    final int k;
    final boolean empty;
    final long n;

    //Extract the preamble, assumes at least 8 bytes
    if (srcMem.isReadOnly() && !srcMem.isDirect()) {
      preLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0XFF;
      serVer = srcMem.getByte(SER_VER_BYTE) & 0XFF;
      familyID = srcMem.getByte(FAMILY_BYTE) & 0XFF;
      flags = srcMem.getByte(FLAGS_BYTE) & 0XFF;
      k = srcMem.getShort(K_SHORT) & 0XFFFF;

      empty = (flags & EMPTY_FLAG_MASK) > 0;
      n = empty ? 0 : srcMem.getLong(N_LONG);
    } else {
      final Object memObj = srcMem.array(); //may be null
      final long memAdd = srcMem.getCumulativeOffset(0L);

      preLongs = extractPreLongs(memObj, memAdd);
      serVer = extractSerVer(memObj, memAdd);
      familyID = extractFamilyID(memObj, memAdd);
      flags = extractFlags(memObj, memAdd);
      k = extractK(memObj, memAdd);

      empty = (flags & EMPTY_FLAG_MASK) > 0;
      n = empty ? 0 : extractN(memObj, memAdd);
    }

    //VALIDITY CHECKS
    DirectUpdateDoublesSketch.checkPreLongs(preLongs);
    Util.checkFamilyID(familyID);
    DoublesUtil.checkDoublesSerVer(serVer, MIN_DIRECT_DOUBLES_SER_VER);
    //checkDirectCompactFlags(flags); // Must be compact
    checkCompact(serVer, flags);
    Util.checkK(k);
    checkDirectMemCapacity(k, n, memCap);
    DirectUpdateDoublesSketch.checkEmptyAndN(empty, n);

    final DirectCompactDoublesSketch dds = new DirectCompactDoublesSketch(k);
    dds.mem_ = srcMem;
    return dds;
  }

  @Override
  public double getMaxValue() {
    if (mem_.getCapacity() < COMBINED_BUFFER) {
      return Double.NEGATIVE_INFINITY;
    } else {
      return mem_.getDouble(MAX_DOUBLE);
    }
  }

  @Override
  public double getMinValue() {
    if (mem_.getCapacity() < COMBINED_BUFFER) {
      return Double.POSITIVE_INFINITY;
    } else {
      return mem_.getDouble(MIN_DOUBLE);
    }
  }

  @Override
  public long getN() {
    if (mem_.getCapacity() < COMBINED_BUFFER) {
      return 0;
    } else {
      return mem_.getLong(N_LONG);
    }
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public boolean isSameResource(final Memory mem) {
    return MemoryUtil.isSameResource(mem_, mem);
  }

  //Restricted overrides
  //Gets

  @Override
  int getBaseBufferCount() {
    return computeBaseBufferItems(getK(), getN());
  }

  @Override
  int getCombinedBufferItemCapacity() {
    return ((int)mem_.getCapacity() - COMBINED_BUFFER) / 8;
  }

  @Override
  double[] getCombinedBuffer() {
    final int k = getK();
    if (isEmpty()) { return new double[k << 1]; } //2K
    final long n = getN();
    final int itemCap = computeRetainedItems(k, n);
    final double[] combinedBuffer = new double[itemCap];
    mem_.getDoubleArray(COMBINED_BUFFER, combinedBuffer, 0, itemCap);
    return combinedBuffer;
  }

  @Override
  long getBitPattern() {
    final int k = getK();
    final long n = getN();
    return computeBitPattern(k, n);
  }

  @Override
  Memory getMemory() {
    return mem_;
  }

  //Checks

  /**
   * Checks the validity of the direct memory capacity assuming n, k.
   * @param k the given value of k
   * @param n the given value of n
   * @param memCapBytes the current memory capacity in bytes
   */
  static void checkDirectMemCapacity(final int k, final long n, final long memCapBytes) {
    final int reqBufBytes = getCompactStorageBytes(k, n);

    if (memCapBytes < reqBufBytes) {
      throw new SketchesArgumentException("Possible corruption: Memory capacity too small: "
          + memCapBytes + " < " + reqBufBytes);
    }
  }

  /**
   * Checks a sketch's serial version and flags to see if the sketch can be wrapped as a
   * DirectCompactDoubleSketch. Throws an exception if the sketch is neither empty nor compact
   * and ordered, unles the sketch uses serialization version 2.
   * @param flags Flags from the sketch to evaluate
   */
  static void checkCompact(final int serVer, final int flags) {
    final int compactFlagMask = COMPACT_FLAG_MASK | ORDERED_FLAG_MASK;
    if ((serVer != 2)
            && ((flags & EMPTY_FLAG_MASK) == 0)
            && ((flags & compactFlagMask) != compactFlagMask)) {
      throw new SketchesArgumentException(
              "Possible corruption: Must be v2, empty, or compact and ordered. Flags field: "
                      + Integer.toBinaryString(flags) + ", SerVer: " + serVer);
    }
  }
}
