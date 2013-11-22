package headwater.bitmap;

import java.util.BitSet;

/**
 * Composes a java.util.BetSet and exposes just the stuff I want.
 * Endianess is thus:
 * Hi..............Lo
 */
public class JuBitmap extends AbstractBitmap {
    
    private static final int BITS_IN_BYTE = 8;
    
    private final int size;
    private final BitSet bits;
    
    
    private JuBitmap(BitSet bits, int size) {
        this.size = size;
        this.bits = bits;
    }
    
    public JuBitmap(int bitSize) {
        if (bitSize % 8 != 0)
            throw new IllegalArgumentException("getBitLength must be evenly divisible by 8");
        this.size = bitSize;
        bits = new BitSet(bitSize);
    }

    public void clear() {
        bits.clear();
    }

    public boolean isEmpty() {
        return bits.isEmpty();
    }

    public int getBitLength() { return size; }
    
    public void set(int bit, boolean value) {
        bits.set(bit, value);
    }
    
    public void set(int... x) {
        for (int i = 0; i < x.length; i++)
            bits.set(x[i], true);
    }
    
    public int[] getAsserted() {
        int[] asserted = new int[bits.cardinality()];
        int pos = 0;
        int from = 0;
        while (pos < asserted.length) {
            asserted[pos] = bits.nextSetBit(from);
            from = asserted[pos] + 1;
            pos += 1;
        }
        return asserted;
    }

    public boolean get(int bit) {
        return bits.get(bit);
    }

    // returns little endian representation. 
    public byte[] toBytes(int index, int numBytes) {
        // easy way now. hard way later.
        byte[] buf = toBytes();
        
        // save a copy if this is the whole thing. it happens.
        if (numBytes == getBitLength() / 8)
            return buf;
        
        byte[] copy = new byte[numBytes];
        System.arraycopy(buf, index, copy, 0, numBytes);
        return copy;
    }
    
    // returns little endian representation.
    public byte[] toBytes() {
        byte[] notPadded = bits.toByteArray();
        if (notPadded.length < size / 8) {
            byte[] all = new byte[size / 8];
            System.arraycopy(notPadded, 0, all, 0, notPadded.length);
            return all;
        } else {
            return notPadded;
        }
    }
    
    public String toString() {
        return bits.toString();
    }

    public Object clone() {
        return new JuBitmap((BitSet)this.bits.clone(), this.size);
    }

    public static JuBitmap fromBytes(byte... b) {
        BitSet bs = BitSet.valueOf(b);
        return new JuBitmap(bs, b.length * BITS_IN_BYTE);
    }
    
    public static final BitmapFactory FACTORY = new BitmapFactory() {
        public IBitmap newBitmap(int numBits) {
            return new JuBitmap(numBits);
        }
    };
}
