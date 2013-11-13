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

    public int getBitLength() {
        // todo: address in APIs.
        return (int)this.numBits;
    }

    public void set(int bit, boolean value) {
        if (value)
            bits.set((long)bit);
        else
            bits.clear((long)bit);
    }

    public void set(int... bits) {
        for (int bit : bits)
            this.set(bit, true);
    }

    public boolean get(int bit) {
        return bits.get((long)bit);
    }

    public int[] getAsserted() {
        int[] asserted = new int[(int)bits.cardinality()];
        int pos = 0;
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
}
