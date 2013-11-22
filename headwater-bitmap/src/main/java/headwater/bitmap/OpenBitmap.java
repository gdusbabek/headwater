package headwater.bitmap;

// todo: iterate until there are no casts.
public class OpenBitmap extends AbstractBitmap {
    
    private final OpenBitSet bits;
    private final long numBits;
    
    public OpenBitmap(int numBits) {
        if (numBits % 8 != 0)
            throw new IllegalArgumentException("getBitLength must be evenly divisible by 8");
        this.numBits = numBits;
        bits = new OpenBitSet(numBits);
    }
    
    private OpenBitmap(int numBits, OpenBitSet bits) {
        this.bits = bits;
        this.numBits = numBits;
    }
    
    @Override
    public Object clone() {
        OpenBitmap clone = new OpenBitmap((int)this.numBits, (OpenBitSet)this.bits.clone());
        return clone;
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
    
    public static final BitmapFactory FACTORY = new BitmapFactory() {
        public IBitmap newBitmap(int numBits) {
            return new OpenBitmap(numBits); 
        }

        public IBitmap newBitmap() {
            return new OpenBitmap(128);
        }
    };
}
