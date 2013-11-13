package headwater.bitmap;


/** part of a bitmap, ready for storage */
public class Chunk {
    // does not refer back to the original bitmap. this indicates that this is the offsetth chunk.
    private final int offset;
    private final byte[] values;
    
    public Chunk(int offset, byte[] values) {
        this.offset = offset;
        this.values = values;
    }
    
    public int getOffset() { return offset; }
    public byte[] getValues() { return values; }
}
