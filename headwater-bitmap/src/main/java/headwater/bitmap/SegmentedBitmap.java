package headwater.bitmap;

import java.util.ArrayList;
import java.util.List;

/** space efficient bitmap implmementation. todo: has concurrency issues that should be addressed */
public class SegmentedBitmap extends AbstractBitmap {
    private final long bitLength;
    private final int chunkBitLength;
    
    // most significant maps are stored at the beginning.
    private final IBitmap[] maps;
    private final BitmapFactory bitmapFactory;
    
    
    public SegmentedBitmap(long bitLength, int chunkBitLength, BitmapFactory bitmapFactory) {
        if (chunkBitLength % 8 != 0)
            throw new IllegalArgumentException("Chunk bit length should be evenly divisible by 8");
        if (bitLength % 8 != 0)
            throw new IllegalArgumentException("Bit length should be evenly divisible by 8");
        if (bitLength % chunkBitLength != 0)
            throw new IllegalArgumentException("Bit length should be evenly divisible by chunk bit length");
            
        this.bitLength = bitLength;  
        this.chunkBitLength = chunkBitLength;
        this.maps = new IBitmap[(int)(bitLength / chunkBitLength)];
        this.bitmapFactory = bitmapFactory;
    }

    public void clear() {
        for (int i = 0; i < maps.length; i++)
            if (maps[i] != null)
                maps[i].clear();
    }

    public boolean isEmpty() {
        for (int i = 0; i < maps.length; i++)
            if (maps[i] != null && !maps[i].isEmpty())
                return false;
        return true;
    }

    public long getBitLength() {
        return bitLength;
    }

    public void set(long bit, boolean value) {
        getSubmap(bit, true).set(getMod(bit), value);
    }

    public void set(long... bits) {
        for (long bit : bits)
            set(bit, true);
    }

    public long[] getAsserted() {
        List<Long> list = new ArrayList<Long>();
        for (int index = 0; index < maps.length; index++) {
            if (maps[index] != null) {
                for (long asserted : maps[index].getAsserted())
                    list.add(index * chunkBitLength + asserted);
            }
        }
        return unbox(list.toArray(new Long[list.size()]));
    }

    public boolean get(long bit) {
        IBitmap map = maps[getIndex(bit)];
        if (map == null) return false;
        else return map.get(getMod(bit));
    }

    public Object clone()  {
        SegmentedBitmap cl = new SegmentedBitmap(this.bitLength, this.chunkBitLength, this.bitmapFactory);
        for (int i = 0; i < maps.length; i++)
            if (maps[i] != null)
                cl.maps[i] = (IBitmap)maps[i].clone();
        return cl;
    }

    // most significant bytes at the end, least significant bytes first.
    public byte[] toBytes() {
        byte[] buf = new byte[(int)(bitLength / 8)];
        byte[] sub;
        for (int index = 0; index < maps.length; index++) {
            int copyPos = index * (chunkBitLength / 8);
            if (maps[index] != null) {
                sub = maps[index].toBytes();
                System.arraycopy(sub, 0, buf, copyPos, sub.length);
            }
        }
        return buf;
    }

    public byte[] toBytes(int byteStart, int numBytes) {
        byte[] buf = new byte[numBytes];
        int bufPos = 0;
        // keep in mind that byteStart may not fall on a buf boundary.
        final int chunkByteLen = chunkBitLength / 8;
        int bytesLate = ((byteStart * 8) % chunkBitLength) / 8;
        
        while (bufPos < numBytes) {
            // increment will be one of 1) number of bytes in partial first buffer, 2) all bytes in middle buffer
            // 3) number of bytes in partial last buffer.
            int inc = bytesLate > 0 ? (chunkByteLen - bytesLate) : Math.min(chunkByteLen, numBytes - bufPos);
            int bit = (byteStart + bufPos) * 8;
            IBitmap map = getSubmap(bit, false);
            if (map != null) {
                byte[] sub = map.toBytes(bytesLate, inc);
                System.arraycopy(sub, 0, buf, bufPos, Math.min(sub.length, chunkByteLen - bufPos + 1));
            }
            bufPos += inc;
            bytesLate = 0;
        }
        return buf;
    }

    private int getSize() {
        return chunkBitLength / 8 * maps.length;
    }

    //
    // helpers
    //
    
    private static long[] unbox(Long[] arr) {
        long[] newarr = new long[arr.length];
        for (int i = 0; i < newarr.length; i++)
            newarr[i] = arr[i];
        return newarr;
    }
    
    // todo: a lot could be done to make this more concurrent. segmented locks would be a good first step.
    private synchronized IBitmap getSubmap(long bit, boolean constructive) {
        int index = getIndex(bit);
        if (maps[index] == null) {
            if (constructive)
                maps[index] = bitmapFactory.newBitmap(chunkBitLength);
            else
                return null;
        }
        return maps[index];
    }
    
    private int getIndex(long bit) {
        return (int)(bit / chunkBitLength);
    }
    
    private int getMod(long bit) {
        return (int)(bit % chunkBitLength);
    }
    
    private long unMod(int nth, int index) {
        return nth * chunkBitLength + index;
    }
}
