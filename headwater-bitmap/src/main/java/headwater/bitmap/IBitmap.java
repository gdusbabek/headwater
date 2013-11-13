package headwater.bitmap;

public interface IBitmap extends Cloneable {
    
    /** number of bits in this map. */
    public int getBitLength();
    
    /** turn this bit on/off */
    public void set(int bit, boolean value);
    
    /** assert these bits (convenience) */
    public void set(int... bits);
    
    public boolean get(int bit);
    
    /** returns which bits are asserted */
    public int[] getAsserted();
    
    public void clear();
    
    public boolean isEmpty();
    
    public Object clone();
    
    // mutating and non-mutating AND and OR operations.
    public IBitmap mand(IBitmap other);
    public IBitmap and(IBitmap other);
    public IBitmap mor(IBitmap other);
    public IBitmap or(IBitmap other);
    
    // potentially wasteful.
    public byte[] toBytes();
    
    public byte[] toBytes(int byteStart, int numBytes);
}
