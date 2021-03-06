package headwater.bitmap;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Lineage: This code was copied from the Cassandra 1.2 source distribution. 
 */

/**
 * An "open" BitSet implementation that allows direct access to the arrays of words
 * storing the bits.  Derived from Lucene's OpenBitSet, but with a paged backing array
 * (see bits delaration, below).
 * <p/>
 * Unlike java.util.bitset, the fact that bits are packed into an array of longs
 * is part of the interface.  This allows efficient implementation of other algorithms
 * by someone other than the author.  It also allows one to efficiently implement
 * alternate serialization or interchange formats.
 * <p/>
 * <code>OpenBitSet</code> is faster than <code>java.util.BitSet</code> in most operations
 * and *much* faster at calculating cardinality of sets and results of set operations.
 * It can also handle sets of larger cardinality (up to 64 * 2**32-1)
 * <p/>
 * The goals of <code>OpenBitSet</code> are the fastest implementation possible, and
 * maximum code reuse.  Extra safety and encapsulation
 * may always be built on top, but if that's built in, the cost can never be removed (and
 * hence people re-implement their own version in order to get better performance).
 * If you want a "safe", totally encapsulated (and slower and limited) BitSet
 * class, use <code>java.util.BitSet</code>.
 */

public class OpenBitSet implements Serializable, Cloneable {
  /**
   * We break the bitset up into multiple arrays to avoid promotion failure caused by attempting to allocate
   * large, contiguous arrays (CASSANDRA-2466).  All sub-arrays but the last are uniformly PAGE_SIZE words;
   * to avoid waste in small bloom filters (of which Cassandra has many: one per row) the last sub-array
   * is sized to exactly the remaining number of words required to achieve the desired set size (CASSANDRA-3618).
   */
  private final long[][] bits;
  private int wlen; // number of words (elements) used in the array
  private final int pageCount;
  private static final int PAGE_SIZE = 4096;

  /**
   * Constructs an OpenBitSet large enough to hold numBits.
   * @param numBits
   */
  public OpenBitSet(long numBits)
  {
      wlen = bits2words(numBits);
      int lastPageSize = wlen % PAGE_SIZE;
      int fullPageCount = wlen / PAGE_SIZE;
      pageCount = fullPageCount + (lastPageSize == 0 ? 0 : 1);

      bits = new long[pageCount][];

      for (int i = 0; i < fullPageCount; ++i)
          bits[i] = new long[PAGE_SIZE];

      if (lastPageSize != 0)
          bits[bits.length - 1] = new long[lastPageSize];
  }

  public OpenBitSet() {
    this(64);
  }

  /**
   * @return the pageSize
   */
  public int getPageSize()
  {
      return PAGE_SIZE;
  }

  public int getPageCount()
  {
      return pageCount;
  }

  public long[] getPage(int pageIdx)
  {
      return bits[pageIdx];
  }

  /** Contructs an OpenBitSet from a BitSet
  */
  public OpenBitSet(BitSet bits) {
    this(bits.length());
  }

  /** Returns the current capacity in bits (1 greater than the index of the last bit) */
  public long capacity() { return ((long)wlen) << 6; }

 /**
  * Returns the current capacity of this set.  Included for
  * compatibility.  This is *not* equal to {@link #cardinality}
  */
  public long size() {
      return capacity();
  }

  // @Override -- not until Java 1.6
  public long length() {
    return capacity();
  }

  /** Returns true if there are no set bits */
  public boolean isEmpty() { return cardinality()==0; }


  /** Expert: gets the number of longs in the array that are in use */
  public int getNumWords() { return wlen; }


  /**
   * Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size
   */
  public boolean get(int index) {
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    int bit = index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    // TODO perfectionist one can implement this using bit operations
    return (bits[i / PAGE_SIZE][i % PAGE_SIZE ] & bitmask) != 0;
  }

  /**
   * Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size.
   */
  public boolean get(long index) {
    int i = (int)(index >> 6);               // div 64
    int bit = (int)index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    // TODO perfectionist one can implement this using bit operations
    return (bits[i / PAGE_SIZE][i % PAGE_SIZE ] & bitmask) != 0;
  }

  /** returns 1 if the bit is set, 0 if not.
   * The index should be less than the OpenBitSet size
   */
  public int getBit(int index) {
    int i = index >> 6;                // div 64
    int bit = index & 0x3f;            // mod 64
    return ((int)(bits[i / PAGE_SIZE][i % PAGE_SIZE ]>>>bit)) & 0x01;
  }

  /**
   * Sets the bit at the specified index.
   * The index should be less than the OpenBitSet size.
   */
  public void set(long index) {
    int wordNum = (int)(index >> 6);
    int bit = (int)index & 0x3f;
    long bitmask = 1L << bit;
    bits[ wordNum / PAGE_SIZE ][ wordNum % PAGE_SIZE ] |= bitmask;
  }

  /**
   * Sets the bit at the specified index.
   * The index should be less than the OpenBitSet size.
   */
  public void set(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[ wordNum / PAGE_SIZE ][ wordNum % PAGE_SIZE ] |= bitmask;
  }

  /**
   * clears a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void clear(int index) {
    int wordNum = index >> 6;
    int bit = index & 0x03f;
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] &= ~bitmask;
    // hmmm, it takes one more instruction to clear than it does to set... any
    // way to work around this?  If there were only 63 bits per word, we could
    // use a right shift of 10111111...111 in binary to position the 0 in the
    // correct place (using sign extension).
    // Could also use Long.rotateRight() or rotateLeft() *if* they were converted
    // by the JVM into a native instruction.
    // bits[word] &= Long.rotateLeft(0xfffffffe,bit);
  }

  /**
   * clears a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void clear(long index) {
    int wordNum = (int)(index >> 6); // div 64
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] &= ~bitmask;
  }

  /**
   * Clears a range of bits.  Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public void clear(int startIndex, int endIndex) {
    if (endIndex <= startIndex) return;

    int startWord = (startIndex>>6);
    if (startWord >= wlen) return;

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord   = ((endIndex-1)>>6);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
      bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] &= (startmask | endmask);
      return;
    }


    bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE]  &= startmask;

    int middle = Math.min(wlen, endWord);
    if (startWord / PAGE_SIZE == middle / PAGE_SIZE)
    {
        Arrays.fill(bits[startWord / PAGE_SIZE], (startWord + 1) % PAGE_SIZE, middle % PAGE_SIZE, 0L);
    } else
    {
        while (++startWord<middle)
            bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] = 0L;
    }
    if (endWord < wlen) {
      bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] &= endmask;
    }
  }


  /** Clears a range of bits.  Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public void clear(long startIndex, long endIndex) {
    if (endIndex <= startIndex) return;

    int startWord = (int)(startIndex>>6);
    if (startWord >= wlen) return;

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord   = (int)((endIndex-1)>>6);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
        bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] &= (startmask | endmask);
        return;
    }

    bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE]  &= startmask;

    int middle = Math.min(wlen, endWord);
    if (startWord / PAGE_SIZE == middle / PAGE_SIZE)
    {
        Arrays.fill(bits[startWord/PAGE_SIZE], (startWord+1) % PAGE_SIZE, middle % PAGE_SIZE, 0L);
    } else
    {
        while (++startWord<middle)
            bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] = 0L;
    }
    if (endWord < wlen) {
        bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] &= endmask;
    }
  }



  /** Sets a bit and returns the previous value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean getAndSet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] & bitmask) != 0;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] |= bitmask;
    return val;
  }

  /** Sets a bit and returns the previous value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean getAndSet(long index) {
    int wordNum = (int)(index >> 6);      // div 64
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] & bitmask) != 0;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] |= bitmask;
    return val;
  }

  /** flips a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void flip(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] ^= bitmask;
  }

  /**
   * flips a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void flip(long index) {
    int wordNum = (int)(index >> 6);   // div 64
    int bit = (int)index & 0x3f;       // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] ^= bitmask;
  }

  /** flips a bit and returns the resulting bit value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean flipAndGet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] ^= bitmask;
    return (bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] & bitmask) != 0;
  }

  /** flips a bit and returns the resulting bit value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean flipAndGet(long index) {
    int wordNum = (int)(index >> 6);   // div 64
    int bit = (int)index & 0x3f;       // mod 64
    long bitmask = 1L << bit;
    bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] ^= bitmask;
    return (bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] & bitmask) != 0;
  }

  /** @return the number of set bits */
  public long cardinality()
  {
    long bitCount = 0L;
    for (int i=getPageCount();i-->0;)
        bitCount+=BitUtil.pop_array(bits[i],0,bits[i].length);

    return bitCount;
  }

  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public int nextSetBit(int index) {
    int i = index>>6;
    if (i>=wlen) return -1;
    int subIndex = index & 0x3f;      // index within the word
    long word = bits[i / PAGE_SIZE][ i % PAGE_SIZE] >> subIndex;  // skip all the bits to the right of index

    if (word!=0) {
      return (i<<6) + subIndex + BitUtil.ntz(word);
    }

    while(++i < wlen) {
      word = bits[i / PAGE_SIZE][i % PAGE_SIZE];
      if (word!=0) return (i<<6) + BitUtil.ntz(word);
    }

    return -1;
  }

  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public long nextSetBit(long index) {
    int i = (int)(index>>>6);
    if (i>=wlen) return -1;
    int subIndex = (int)index & 0x3f; // index within the word
    long word = bits[i / PAGE_SIZE][i % PAGE_SIZE] >>> subIndex;  // skip all the bits to the right of index

    if (word!=0) {
      return (((long)i)<<6) + (subIndex + BitUtil.ntz(word));
    }

    while(++i < wlen) {
      word = bits[i / PAGE_SIZE][i % PAGE_SIZE];
      if (word!=0) return (((long)i)<<6) + BitUtil.ntz(word);
    }

    return -1;
  }

  /** this = this AND other */
  public void intersect(OpenBitSet other) {
    int newLen= Math.min(this.wlen,other.wlen);
    long[][] thisArr = this.bits;
    long[][] otherArr = other.bits;
    int thisPageSize = this.PAGE_SIZE;
    int otherPageSize = other.PAGE_SIZE;
    // testing against zero can be more efficient
    int pos=newLen;
    while(--pos>=0) {
      thisArr[pos / thisPageSize][ pos % thisPageSize] &= otherArr[pos / otherPageSize][pos % otherPageSize];
    }

    if (this.wlen > newLen) {
      // fill zeros from the new shorter length to the old length
      for (pos=wlen;pos-->newLen;)
          thisArr[pos / thisPageSize][ pos % thisPageSize] =0;
    }
    this.wlen = newLen;
  }

  // some BitSet compatability methods

  //** see {@link intersect} */
  public void and(OpenBitSet other) {
    intersect(other);
  }

  /** Lowers numWords, the number of words in use,
   * by checking for trailing zero words.
   */
  public void trimTrailingZeros() {
    int idx = wlen-1;
    while (idx>=0 && bits[idx / PAGE_SIZE][idx % PAGE_SIZE]==0) idx--;
    wlen = idx+1;
  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(long numBits) {
   return (int)(((numBits-1)>>>6)+1);
  }

  /** returns true if both sets have the same bits set */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OpenBitSet)) return false;
    OpenBitSet a;
    OpenBitSet b = (OpenBitSet)o;
    // make a the larger set.
    if (b.wlen > this.wlen) {
      a = b; b=this;
    } else {
      a=this;
    }

    int aPageSize = this.PAGE_SIZE;
    int bPageSize = b.PAGE_SIZE;

    // check for any set bits out of the range of b
    for (int i=a.wlen-1; i>=b.wlen; i--) {
      if (a.bits[i/aPageSize][i % aPageSize]!=0) return false;
    }

    for (int i=b.wlen-1; i>=0; i--) {
      if (a.bits[i/aPageSize][i % aPageSize] != b.bits[i/bPageSize][i % bPageSize]) return false;
    }

    return true;
  }


  @Override
  public int hashCode() {
    // Start with a zero hash and use a mix that results in zero if the input is zero.
    // This effectively truncates trailing zeros without an explicit check.
    long h = 0;
    for (int i = wlen; --i>=0;) {
      h ^= bits[i / PAGE_SIZE][i % PAGE_SIZE];
      h = (h << 1) | (h >>> 63); // rotate left
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int)((h>>32) ^ h) + 0x98761234;
  }
    
    
    // ADDED LATER FOR HEADWATER

    @Override
    protected Object clone() {
        OpenBitSet clone = new OpenBitSet(this.wlen * 64);
        for (int i = 0; i < this.bits.length; i++)
            System.arraycopy(this.bits[i], 0, clone.bits[i], 0, this.bits[i].length);
        return clone;
    }
    
    // todo: clean this up and optimize.
    public byte[] toByteArray() {
        // we're going to do this the stupid way first.
        int byteCount = 0;
        List<byte[]> buffers = new ArrayList<byte[]>(bits.length);
        for (long[] part : bits) {
            BitSet bs = BitSet.valueOf(part);
            byte[] buf = bs.toByteArray();
            byteCount += buf.length;
            buffers.add(buf);
        }
        
        byte[] buf = new byte[byteCount];
        int pos = 0;
        for (byte[] part : buffers) {
            System.arraycopy(part, 0, buf, pos, part.length);
            pos += part.length;
        }
        return buf;
    }

    
    /**  A variety of high efficiency bit twiddling routines.
     * @lucene.internal
     */
    public final static class BitUtil {
    
      /** Returns the number of bits set in the long */
      public static int pop(long x) {
      /* Hacker's Delight 32 bit pop function:
       * http://www.hackersdelight.org/HDcode/newCode/pop_arrayHS.cc
       *
      int pop(unsigned x) {
         x = x - ((x >> 1) & 0x55555555);
         x = (x & 0x33333333) + ((x >> 2) & 0x33333333);
         x = (x + (x >> 4)) & 0x0F0F0F0F;
         x = x + (x >> 8);
         x = x + (x >> 16);
         return x & 0x0000003F;
        }
      ***/
    
        // 64 bit java version of the C function from above
        x = x - ((x >>> 1) & 0x5555555555555555L);
        x = (x & 0x3333333333333333L) + ((x >>>2 ) & 0x3333333333333333L);
        x = (x + (x >>> 4)) & 0x0F0F0F0F0F0F0F0FL;
        x = x + (x >>> 8);
        x = x + (x >>> 16);
        x = x + (x >>> 32);
        return ((int)x) & 0x7F;
      }
    
      /*** Returns the number of set bits in an array of longs. */
      public static long pop_array(long A[], int wordOffset, int numWords) {
        /*
        * Robert Harley and David Seal's bit counting algorithm, as documented
        * in the revisions of Hacker's Delight
        * http://www.hackersdelight.org/revisions.pdf
        * http://www.hackersdelight.org/HDcode/newCode/pop_arrayHS.cc
        *
        * This function was adapted to Java, and extended to use 64 bit words.
        * if only we had access to wider registers like SSE from java...
        *
        * This function can be transformed to compute the popcount of other functions
        * on bitsets via something like this:
        * sed 's/A\[\([^]]*\)\]/\(A[\1] \& B[\1]\)/g'
        *
        */
        int n = wordOffset+numWords;
        long tot=0, tot8=0;
        long ones=0, twos=0, fours=0;
    
        int i;
        for (i = wordOffset; i <= n - 8; i+=8) {
          /***  C macro from Hacker's Delight
           #define CSA(h,l, a,b,c) \
           {unsigned u = a ^ b; unsigned v = c; \
           h = (a & b) | (u & v); l = u ^ v;}
           ***/
    
          long twosA,twosB,foursA,foursB,eights;
    
          // CSA(twosA, ones, ones, A[i], A[i+1])
          {
            long b=A[i], c=A[i+1];
            long u=ones ^ b;
            twosA=(ones & b)|( u & c);
            ones=u^c;
          }
          // CSA(twosB, ones, ones, A[i+2], A[i+3])
          {
            long b=A[i+2], c=A[i+3];
            long u=ones^b;
            twosB =(ones&b)|(u&c);
            ones=u^c;
          }
          //CSA(foursA, twos, twos, twosA, twosB)
          {
            long u=twos^twosA;
            foursA=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
          //CSA(twosA, ones, ones, A[i+4], A[i+5])
          {
            long b=A[i+4], c=A[i+5];
            long u=ones^b;
            twosA=(ones&b)|(u&c);
            ones=u^c;
          }
          // CSA(twosB, ones, ones, A[i+6], A[i+7])
          {
            long b=A[i+6], c=A[i+7];
            long u=ones^b;
            twosB=(ones&b)|(u&c);
            ones=u^c;
          }
          //CSA(foursB, twos, twos, twosA, twosB)
          {
            long u=twos^twosA;
            foursB=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
    
          //CSA(eights, fours, fours, foursA, foursB)
          {
            long u=fours^foursA;
            eights=(fours&foursA)|(u&foursB);
            fours=u^foursB;
          }
          tot8 += pop(eights);
        }
    
        // handle trailing words in a binary-search manner...
        // derived from the loop above by setting specific elements to 0.
        // the original method in Hackers Delight used a simple for loop:
        //   for (i = i; i < n; i++)      // Add in the last elements
        //  tot = tot + pop(A[i]);
    
        if (i<=n-4) {
          long twosA, twosB, foursA, eights;
          {
            long b=A[i], c=A[i+1];
            long u=ones ^ b;
            twosA=(ones & b)|( u & c);
            ones=u^c;
          }
          {
            long b=A[i+2], c=A[i+3];
            long u=ones^b;
            twosB =(ones&b)|(u&c);
            ones=u^c;
          }
          {
            long u=twos^twosA;
            foursA=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
          eights=fours&foursA;
          fours=fours^foursA;
    
          tot8 += pop(eights);
          i+=4;
        }
    
        if (i<=n-2) {
          long b=A[i], c=A[i+1];
          long u=ones ^ b;
          long twosA=(ones & b)|( u & c);
          ones=u^c;
    
          long foursA=twos&twosA;
          twos=twos^twosA;
    
          long eights=fours&foursA;
          fours=fours^foursA;
    
          tot8 += pop(eights);
          i+=2;
        }
    
        if (i<n) {
          tot += pop(A[i]);
        }
    
        tot += (pop(fours)<<2)
                + (pop(twos)<<1)
                + pop(ones)
                + (tot8<<3);
    
        return tot;
      }
    
      /** Returns the popcount or cardinality of the two sets after an intersection.
       * Neither array is modified.
       */
      public static long pop_intersect(long A[], long B[], int wordOffset, int numWords) {
        // generated from pop_array via sed 's/A\[\([^]]*\)\]/\(A[\1] \& B[\1]\)/g'
        int n = wordOffset+numWords;
        long tot=0, tot8=0;
        long ones=0, twos=0, fours=0;
    
        int i;
        for (i = wordOffset; i <= n - 8; i+=8) {
          long twosA,twosB,foursA,foursB,eights;
    
          // CSA(twosA, ones, ones, (A[i] & B[i]), (A[i+1] & B[i+1]))
          {
            long b=(A[i] & B[i]), c=(A[i+1] & B[i+1]);
            long u=ones ^ b;
            twosA=(ones & b)|( u & c);
            ones=u^c;
          }
          // CSA(twosB, ones, ones, (A[i+2] & B[i+2]), (A[i+3] & B[i+3]))
          {
            long b=(A[i+2] & B[i+2]), c=(A[i+3] & B[i+3]);
            long u=ones^b;
            twosB =(ones&b)|(u&c);
            ones=u^c;
          }
          //CSA(foursA, twos, twos, twosA, twosB)
          {
            long u=twos^twosA;
            foursA=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
          //CSA(twosA, ones, ones, (A[i+4] & B[i+4]), (A[i+5] & B[i+5]))
          {
            long b=(A[i+4] & B[i+4]), c=(A[i+5] & B[i+5]);
            long u=ones^b;
            twosA=(ones&b)|(u&c);
            ones=u^c;
          }
          // CSA(twosB, ones, ones, (A[i+6] & B[i+6]), (A[i+7] & B[i+7]))
          {
            long b=(A[i+6] & B[i+6]), c=(A[i+7] & B[i+7]);
            long u=ones^b;
            twosB=(ones&b)|(u&c);
            ones=u^c;
          }
          //CSA(foursB, twos, twos, twosA, twosB)
          {
            long u=twos^twosA;
            foursB=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
    
          //CSA(eights, fours, fours, foursA, foursB)
          {
            long u=fours^foursA;
            eights=(fours&foursA)|(u&foursB);
            fours=u^foursB;
          }
          tot8 += pop(eights);
        }
    
    
        if (i<=n-4) {
          long twosA, twosB, foursA, eights;
          {
            long b=(A[i] & B[i]), c=(A[i+1] & B[i+1]);
            long u=ones ^ b;
            twosA=(ones & b)|( u & c);
            ones=u^c;
          }
          {
            long b=(A[i+2] & B[i+2]), c=(A[i+3] & B[i+3]);
            long u=ones^b;
            twosB =(ones&b)|(u&c);
            ones=u^c;
          }
          {
            long u=twos^twosA;
            foursA=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
          eights=fours&foursA;
          fours=fours^foursA;
    
          tot8 += pop(eights);
          i+=4;
        }
    
        if (i<=n-2) {
          long b=(A[i] & B[i]), c=(A[i+1] & B[i+1]);
          long u=ones ^ b;
          long twosA=(ones & b)|( u & c);
          ones=u^c;
    
          long foursA=twos&twosA;
          twos=twos^twosA;
    
          long eights=fours&foursA;
          fours=fours^foursA;
    
          tot8 += pop(eights);
          i+=2;
        }
    
        if (i<n) {
          tot += pop((A[i] & B[i]));
        }
    
        tot += (pop(fours)<<2)
                + (pop(twos)<<1)
                + pop(ones)
                + (tot8<<3);
    
        return tot;
      }
    
      /** Returns the popcount or cardinality of the union of two sets.
        * Neither array is modified.
        */
       public static long pop_union(long A[], long B[], int wordOffset, int numWords) {
         // generated from pop_array via sed 's/A\[\([^]]*\)\]/\(A[\1] \| B[\1]\)/g'
         int n = wordOffset+numWords;
         long tot=0, tot8=0;
         long ones=0, twos=0, fours=0;
    
         int i;
         for (i = wordOffset; i <= n - 8; i+=8) {
           /***  C macro from Hacker's Delight
            #define CSA(h,l, a,b,c) \
            {unsigned u = a ^ b; unsigned v = c; \
            h = (a & b) | (u & v); l = u ^ v;}
            ***/
    
           long twosA,twosB,foursA,foursB,eights;
    
           // CSA(twosA, ones, ones, (A[i] | B[i]), (A[i+1] | B[i+1]))
           {
             long b=(A[i] | B[i]), c=(A[i+1] | B[i+1]);
             long u=ones ^ b;
             twosA=(ones & b)|( u & c);
             ones=u^c;
           }
           // CSA(twosB, ones, ones, (A[i+2] | B[i+2]), (A[i+3] | B[i+3]))
           {
             long b=(A[i+2] | B[i+2]), c=(A[i+3] | B[i+3]);
             long u=ones^b;
             twosB =(ones&b)|(u&c);
             ones=u^c;
           }
           //CSA(foursA, twos, twos, twosA, twosB)
           {
             long u=twos^twosA;
             foursA=(twos&twosA)|(u&twosB);
             twos=u^twosB;
           }
           //CSA(twosA, ones, ones, (A[i+4] | B[i+4]), (A[i+5] | B[i+5]))
           {
             long b=(A[i+4] | B[i+4]), c=(A[i+5] | B[i+5]);
             long u=ones^b;
             twosA=(ones&b)|(u&c);
             ones=u^c;
           }
           // CSA(twosB, ones, ones, (A[i+6] | B[i+6]), (A[i+7] | B[i+7]))
           {
             long b=(A[i+6] | B[i+6]), c=(A[i+7] | B[i+7]);
             long u=ones^b;
             twosB=(ones&b)|(u&c);
             ones=u^c;
           }
           //CSA(foursB, twos, twos, twosA, twosB)
           {
             long u=twos^twosA;
             foursB=(twos&twosA)|(u&twosB);
             twos=u^twosB;
           }
    
           //CSA(eights, fours, fours, foursA, foursB)
           {
             long u=fours^foursA;
             eights=(fours&foursA)|(u&foursB);
             fours=u^foursB;
           }
           tot8 += pop(eights);
         }
    
    
         if (i<=n-4) {
           long twosA, twosB, foursA, eights;
           {
             long b=(A[i] | B[i]), c=(A[i+1] | B[i+1]);
             long u=ones ^ b;
             twosA=(ones & b)|( u & c);
             ones=u^c;
           }
           {
             long b=(A[i+2] | B[i+2]), c=(A[i+3] | B[i+3]);
             long u=ones^b;
             twosB =(ones&b)|(u&c);
             ones=u^c;
           }
           {
             long u=twos^twosA;
             foursA=(twos&twosA)|(u&twosB);
             twos=u^twosB;
           }
           eights=fours&foursA;
           fours=fours^foursA;
    
           tot8 += pop(eights);
           i+=4;
         }
    
         if (i<=n-2) {
           long b=(A[i] | B[i]), c=(A[i+1] | B[i+1]);
           long u=ones ^ b;
           long twosA=(ones & b)|( u & c);
           ones=u^c;
    
           long foursA=twos&twosA;
           twos=twos^twosA;
    
           long eights=fours&foursA;
           fours=fours^foursA;
    
           tot8 += pop(eights);
           i+=2;
         }
    
         if (i<n) {
           tot += pop((A[i] | B[i]));
         }
    
         tot += (pop(fours)<<2)
                 + (pop(twos)<<1)
                 + pop(ones)
                 + (tot8<<3);
    
         return tot;
       }
    
      /** Returns the popcount or cardinality of A & ~B
       * Neither array is modified.
       */
      public static long pop_andnot(long A[], long B[], int wordOffset, int numWords) {
        // generated from pop_array via sed 's/A\[\([^]]*\)\]/\(A[\1] \& ~B[\1]\)/g'
        int n = wordOffset+numWords;
        long tot=0, tot8=0;
        long ones=0, twos=0, fours=0;
    
        int i;
        for (i = wordOffset; i <= n - 8; i+=8) {
          /***  C macro from Hacker's Delight
           #define CSA(h,l, a,b,c) \
           {unsigned u = a ^ b; unsigned v = c; \
           h = (a & b) | (u & v); l = u ^ v;}
           ***/
    
          long twosA,twosB,foursA,foursB,eights;
    
          // CSA(twosA, ones, ones, (A[i] & ~B[i]), (A[i+1] & ~B[i+1]))
          {
            long b=(A[i] & ~B[i]), c=(A[i+1] & ~B[i+1]);
            long u=ones ^ b;
            twosA=(ones & b)|( u & c);
            ones=u^c;
          }
          // CSA(twosB, ones, ones, (A[i+2] & ~B[i+2]), (A[i+3] & ~B[i+3]))
          {
            long b=(A[i+2] & ~B[i+2]), c=(A[i+3] & ~B[i+3]);
            long u=ones^b;
            twosB =(ones&b)|(u&c);
            ones=u^c;
          }
          //CSA(foursA, twos, twos, twosA, twosB)
          {
            long u=twos^twosA;
            foursA=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
          //CSA(twosA, ones, ones, (A[i+4] & ~B[i+4]), (A[i+5] & ~B[i+5]))
          {
            long b=(A[i+4] & ~B[i+4]), c=(A[i+5] & ~B[i+5]);
            long u=ones^b;
            twosA=(ones&b)|(u&c);
            ones=u^c;
          }
          // CSA(twosB, ones, ones, (A[i+6] & ~B[i+6]), (A[i+7] & ~B[i+7]))
          {
            long b=(A[i+6] & ~B[i+6]), c=(A[i+7] & ~B[i+7]);
            long u=ones^b;
            twosB=(ones&b)|(u&c);
            ones=u^c;
          }
          //CSA(foursB, twos, twos, twosA, twosB)
          {
            long u=twos^twosA;
            foursB=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
    
          //CSA(eights, fours, fours, foursA, foursB)
          {
            long u=fours^foursA;
            eights=(fours&foursA)|(u&foursB);
            fours=u^foursB;
          }
          tot8 += pop(eights);
        }
    
    
        if (i<=n-4) {
          long twosA, twosB, foursA, eights;
          {
            long b=(A[i] & ~B[i]), c=(A[i+1] & ~B[i+1]);
            long u=ones ^ b;
            twosA=(ones & b)|( u & c);
            ones=u^c;
          }
          {
            long b=(A[i+2] & ~B[i+2]), c=(A[i+3] & ~B[i+3]);
            long u=ones^b;
            twosB =(ones&b)|(u&c);
            ones=u^c;
          }
          {
            long u=twos^twosA;
            foursA=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
          eights=fours&foursA;
          fours=fours^foursA;
    
          tot8 += pop(eights);
          i+=4;
        }
    
        if (i<=n-2) {
          long b=(A[i] & ~B[i]), c=(A[i+1] & ~B[i+1]);
          long u=ones ^ b;
          long twosA=(ones & b)|( u & c);
          ones=u^c;
    
          long foursA=twos&twosA;
          twos=twos^twosA;
    
          long eights=fours&foursA;
          fours=fours^foursA;
    
          tot8 += pop(eights);
          i+=2;
        }
    
        if (i<n) {
          tot += pop((A[i] & ~B[i]));
        }
    
        tot += (pop(fours)<<2)
                + (pop(twos)<<1)
                + pop(ones)
                + (tot8<<3);
    
        return tot;
      }
    
      public static long pop_xor(long A[], long B[], int wordOffset, int numWords) {
        int n = wordOffset+numWords;
        long tot=0, tot8=0;
        long ones=0, twos=0, fours=0;
    
        int i;
        for (i = wordOffset; i <= n - 8; i+=8) {
          /***  C macro from Hacker's Delight
           #define CSA(h,l, a,b,c) \
           {unsigned u = a ^ b; unsigned v = c; \
           h = (a & b) | (u & v); l = u ^ v;}
           ***/
    
          long twosA,twosB,foursA,foursB,eights;
    
          // CSA(twosA, ones, ones, (A[i] ^ B[i]), (A[i+1] ^ B[i+1]))
          {
            long b=(A[i] ^ B[i]), c=(A[i+1] ^ B[i+1]);
            long u=ones ^ b;
            twosA=(ones & b)|( u & c);
            ones=u^c;
          }
          // CSA(twosB, ones, ones, (A[i+2] ^ B[i+2]), (A[i+3] ^ B[i+3]))
          {
            long b=(A[i+2] ^ B[i+2]), c=(A[i+3] ^ B[i+3]);
            long u=ones^b;
            twosB =(ones&b)|(u&c);
            ones=u^c;
          }
          //CSA(foursA, twos, twos, twosA, twosB)
          {
            long u=twos^twosA;
            foursA=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
          //CSA(twosA, ones, ones, (A[i+4] ^ B[i+4]), (A[i+5] ^ B[i+5]))
          {
            long b=(A[i+4] ^ B[i+4]), c=(A[i+5] ^ B[i+5]);
            long u=ones^b;
            twosA=(ones&b)|(u&c);
            ones=u^c;
          }
          // CSA(twosB, ones, ones, (A[i+6] ^ B[i+6]), (A[i+7] ^ B[i+7]))
          {
            long b=(A[i+6] ^ B[i+6]), c=(A[i+7] ^ B[i+7]);
            long u=ones^b;
            twosB=(ones&b)|(u&c);
            ones=u^c;
          }
          //CSA(foursB, twos, twos, twosA, twosB)
          {
            long u=twos^twosA;
            foursB=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
    
          //CSA(eights, fours, fours, foursA, foursB)
          {
            long u=fours^foursA;
            eights=(fours&foursA)|(u&foursB);
            fours=u^foursB;
          }
          tot8 += pop(eights);
        }
    
    
        if (i<=n-4) {
          long twosA, twosB, foursA, eights;
          {
            long b=(A[i] ^ B[i]), c=(A[i+1] ^ B[i+1]);
            long u=ones ^ b;
            twosA=(ones & b)|( u & c);
            ones=u^c;
          }
          {
            long b=(A[i+2] ^ B[i+2]), c=(A[i+3] ^ B[i+3]);
            long u=ones^b;
            twosB =(ones&b)|(u&c);
            ones=u^c;
          }
          {
            long u=twos^twosA;
            foursA=(twos&twosA)|(u&twosB);
            twos=u^twosB;
          }
          eights=fours&foursA;
          fours=fours^foursA;
    
          tot8 += pop(eights);
          i+=4;
        }
    
        if (i<=n-2) {
          long b=(A[i] ^ B[i]), c=(A[i+1] ^ B[i+1]);
          long u=ones ^ b;
          long twosA=(ones & b)|( u & c);
          ones=u^c;
    
          long foursA=twos&twosA;
          twos=twos^twosA;
    
          long eights=fours&foursA;
          fours=fours^foursA;
    
          tot8 += pop(eights);
          i+=2;
        }
    
        if (i<n) {
          tot += pop((A[i] ^ B[i]));
        }
    
        tot += (pop(fours)<<2)
                + (pop(twos)<<1)
                + pop(ones)
                + (tot8<<3);
    
        return tot;
      }
    
      /* python code to generate ntzTable
      def ntz(val):
        if val==0: return 8
        i=0
        while (val&0x01)==0:
          i = i+1
          val >>= 1
        return i
      print ','.join([ str(ntz(i)) for i in range(256) ])
      ***/
      /** table of number of trailing zeros in a byte */
      public static final byte[] ntzTable = {8,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,6,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,7,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,6,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,5,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0,4,0,1,0,2,0,1,0,3,0,1,0,2,0,1,0};
    
    
      /** Returns number of trailing zeros in a 64 bit long value. */
      public static int ntz(long val) {
        // A full binary search to determine the low byte was slower than
        // a linear search for nextSetBit().  This is most likely because
        // the implementation of nextSetBit() shifts bits to the right, increasing
        // the probability that the first non-zero byte is in the rhs.
        //
        // This implementation does a single binary search at the top level only
        // so that all other bit shifting can be done on ints instead of longs to
        // remain friendly to 32 bit architectures.  In addition, the case of a
        // non-zero first byte is checked for first because it is the most common
        // in dense bit arrays.
    
        int lower = (int)val;
        int lowByte = lower & 0xff;
        if (lowByte != 0) return ntzTable[lowByte];
    
        if (lower!=0) {
          lowByte = (lower>>>8) & 0xff;
          if (lowByte != 0) return ntzTable[lowByte] + 8;
          lowByte = (lower>>>16) & 0xff;
          if (lowByte != 0) return ntzTable[lowByte] + 16;
          // no need to mask off low byte for the last byte in the 32 bit word
          // no need to check for zero on the last byte either.
          return ntzTable[lower>>>24] + 24;
        } else {
          // grab upper 32 bits
          int upper=(int)(val>>32);
          lowByte = upper & 0xff;
          if (lowByte != 0) return ntzTable[lowByte] + 32;
          lowByte = (upper>>>8) & 0xff;
          if (lowByte != 0) return ntzTable[lowByte] + 40;
          lowByte = (upper>>>16) & 0xff;
          if (lowByte != 0) return ntzTable[lowByte] + 48;
          // no need to mask off low byte for the last byte in the 32 bit word
          // no need to check for zero on the last byte either.
          return ntzTable[upper>>>24] + 56;
        }
      }
    
      /** Returns number of trailing zeros in a 32 bit int value. */
      public static int ntz(int val) {
        // This implementation does a single binary search at the top level only.
        // In addition, the case of a non-zero first byte is checked for first
        // because it is the most common in dense bit arrays.
    
        int lowByte = val & 0xff;
        if (lowByte != 0) return ntzTable[lowByte];
        lowByte = (val>>>8) & 0xff;
        if (lowByte != 0) return ntzTable[lowByte] + 8;
        lowByte = (val>>>16) & 0xff;
        if (lowByte != 0) return ntzTable[lowByte] + 16;
        // no need to mask off low byte for the last byte.
        // no need to check for zero on the last byte either.
        return ntzTable[val>>>24] + 24;
      }
    
      /** returns 0 based index of first set bit
       * (only works for x!=0)
       * <br/> This is an alternate implementation of ntz()
       */
      public static int ntz2(long x) {
       int n = 0;
       int y = (int)x;
       if (y==0) {n+=32; y = (int)(x>>>32); }   // the only 64 bit shift necessary
       if ((y & 0x0000FFFF) == 0) { n+=16; y>>>=16; }
       if ((y & 0x000000FF) == 0) { n+=8; y>>>=8; }
       return (ntzTable[ y & 0xff ]) + n;
      }
    
      /** returns 0 based index of first set bit
       * <br/> This is an alternate implementation of ntz()
       */
      public static int ntz3(long x) {
       // another implementation taken from Hackers Delight, extended to 64 bits
       // and converted to Java.
       // Many 32 bit ntz algorithms are at http://www.hackersdelight.org/HDcode/ntz.cc
       int n = 1;
    
       // do the first step as a long, all others as ints.
       int y = (int)x;
       if (y==0) {n+=32; y = (int)(x>>>32); }
       if ((y & 0x0000FFFF) == 0) { n+=16; y>>>=16; }
       if ((y & 0x000000FF) == 0) { n+=8; y>>>=8; }
       if ((y & 0x0000000F) == 0) { n+=4; y>>>=4; }
       if ((y & 0x00000003) == 0) { n+=2; y>>>=2; }
       return n - (y & 1);
      }
    
    
      /** returns true if v is a power of two or zero*/
      public static boolean isPowerOfTwo(int v) {
        return ((v & (v-1)) == 0);
      }
    
      /** returns true if v is a power of two or zero*/
      public static boolean isPowerOfTwo(long v) {
        return ((v & (v-1)) == 0);
      }
    
      /** returns the next highest power of two, or the current value if it's already a power of two or zero*/
      public static int nextHighestPowerOfTwo(int v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
      }
    
      /** returns the next highest power of two, or the current value if it's already a power of two or zero*/
       public static long nextHighestPowerOfTwo(long v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        v++;
        return v;
      }
    
    }
    
}


