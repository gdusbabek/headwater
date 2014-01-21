package headwater.bitmap;

import java.util.BitSet;

public class MemoryBitmap implements IBitmap {
    private OpenBitSet bits;
    private final long numBits;
    
    public MemoryBitmap(int numBits) {
        if (numBits % 8 != 0)
            throw new IllegalArgumentException("getBitLength must be evenly divisible by 8");
        this.numBits = numBits;
        bits = new OpenBitSet(numBits);
    }
    
    private MemoryBitmap(int numBits, OpenBitSet bits) {
        this.bits = bits;
        this.numBits = numBits;
    }
    
    private MemoryBitmap(byte[] buf) {
        this(buf.length * 8);
        this.setAll(buf);
    }
    
    @Override
    public MemoryBitmap clone() {
        MemoryBitmap clone = new MemoryBitmap((int)this.numBits, (OpenBitSet)this.bits.clone());
        return clone;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof IBitmap))
            return false;
        return MemoryBitmap.bitmapEquals(this, (IBitmap) obj);
    }

    public static boolean bitmapEquals(IBitmap a, IBitmap b) {
        if (a == null && b != null)
            return false;
        if (a != null && b == null)
            return false;
        if (a == null && b == null)
            return true;
        
        if (b.getBitLength() != a.getBitLength())
            return false;
        long[] aAsserted = a.getAsserted();
        long[] bAsserted = b.getAsserted();
        if (aAsserted.length != bAsserted.length)
            return false;
        for (int i = 0; i < aAsserted.length; i++)
            if (aAsserted[i] != bAsserted[i])
                return false;
        return true;
    }

    public long getBitLength() {
        return this.numBits;
    }

    public void set(long bit, boolean value) {
        if (value)
            bits.set(bit);
        else
            bits.clear(bit);
    }

    public void set(long... bits) {
        for (long bit : bits)
            this.set(bit, true);
    }

    public boolean get(long bit) {
        return bits.get(bit);
    }

    public long[] getAsserted() {
        long[] asserted = new long[(int)bits.cardinality()];
        long pos = 0;
        int index = 0;
        while (index < asserted.length) {
            asserted[index] = bits.nextSetBit(pos);
            pos = asserted[index] + 1;
            index += 1;
        }
        return asserted;
    }

    public void clear() {
        bits.clear(0, numBits);
    }

    public boolean isEmpty() {
        return bits.isEmpty();
    }

    public void setAll(byte[] b) {
        // todo: ugh. make this better.
        bits = new OpenBitSet(b.length * 8);
        BitSet bs = BitSet.valueOf(b);
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
            bits.set(i);
        }
    }

    public byte[] toBytes() {
        byte[] notPadded = bits.toByteArray();
        if (notPadded.length < numBits / 8) {
            byte[] all = new byte[(int)numBits / 8];
            System.arraycopy(notPadded, 0, all, 0, notPadded.length);
            return all;
        } else {
            return notPadded;
        }
    }

    // todo: implement this in a smarter way.
    public byte[] toBytes(int byteStart, int numBytes) {
        byte[] buf = toBytes();
        byte[] newBuf = new byte[numBytes];
        System.arraycopy(buf, byteStart, newBuf, 0, numBytes);
        return newBuf;
    }
    
    public static MemoryBitmap wrap(byte[] buf) {
        return new MemoryBitmap(buf);
    }
    
}
