package headwater.bitmap;

public interface IBitmap extends Cloneable {
    
    /** number of bits in this map. */
    public long getBitLength();
    
    /** turn this bit on/off */
    public void set(long bit, boolean value);
    
    /** assert these bits (convenience) */
    public void set(long... bits);
    
    public boolean get(long bit);
    
    /** returns which bits are asserted */
    public long[] getAsserted();
    
    public void setAll(byte[] b);
    
    public void clear();
    
    public boolean isEmpty();
    
    // potentially wasteful.
    public byte[] toBytes();
    
    public byte[] toBytes(int byteStart, int numBytes);
    
    public IBitmap clone() throws CloneNotSupportedException;
}
