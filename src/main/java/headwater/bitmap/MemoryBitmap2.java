package headwater.bitmap;

import headwater.Utils;
import headwater.index.MemoryIndexReader;

public class MemoryBitmap2 implements IBitmap {
    
    private final int bitLength;
    private byte[] buf;
    
    public MemoryBitmap2(int bitLength) {
        if (bitLength % 8 != 0)
            throw new Error("bitLength must be evenly divisible by zero");
        
        this.bitLength = bitLength;
        this.buf = new byte[bitLength / 8];
    }
    
    private MemoryBitmap2(byte[] buf) {
        this.bitLength = buf.length * 8;
        this.buf = buf;
    }
    
    public long getBitLength() {
        return bitLength;
    }

    public void set(long bit, boolean value) {
        if (value)
            Utils.flipOn(buf, bit);
        else
            Utils.flipOff(buf, bit);
    }

    public void set(long... bits) {
        for (long bit : bits)
            set(bit, true);
    }

    public boolean get(long bit) {
        return Utils.isAsserted(buf, bit);
    }

    public long[] getAsserted() {
        return Utils.getAsserted(buf);
    }

    public void setAll(byte[] b) {
        if (b.length != this.buf.length)
            throw new Error("new buffer length must match existing buffer length");
        this.buf = b;
    }

    public void clear() {
        this.buf = new byte[bitLength / 8];
    }

    public boolean isEmpty() {
        for (byte b : buf)
            if (b != 0)
                return false;
        return true;
    }

    public byte[] toBytes() {
        return buf;
    }

    public byte[] toBytes(int byteStart, int numBytes) {
        byte[] copy = new byte[numBytes];
        System.arraycopy(buf, byteStart, copy, 0, numBytes);
        return copy;
    }

    public IBitmap clone() throws CloneNotSupportedException {
        byte[] copy = new byte[buf.length];
        System.arraycopy(buf, 0, copy, 0, copy.length);
        return MemoryBitmap2.wrap(copy);
    }
    
    public static MemoryBitmap2 wrap(byte[] buf) {
        return new MemoryBitmap2(buf);
    }
}
