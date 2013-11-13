package headwater.bitmap;

import junit.framework.Assert;
import org.junit.Test;

public class TestSegmentedBitmap extends AbstractBitmapTest {
    
    public void instantiate() {
        x = new SegmentedBitmap(16, 8);
        y = new SegmentedBitmap(16, 8);
        wide = new SegmentedBitmap(32, 8);
        padded = new SegmentedBitmap(16, 8);
        setZero = new SegmentedBitmap(32, 8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalBitsetLength() {
        new SegmentedBitmap(27, 8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalChunkLength() {
        new SegmentedBitmap(64, 7);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalEverything() {
        new SegmentedBitmap(66, 7);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalDivision() {
        // both are divisible by 8, but you don't get an even number of chunks in 128 bits.
        new SegmentedBitmap(128, 24);
    }
    
    @Test
    public void testWideChunks() {
        IBitmap bm = new SegmentedBitmap(32, 16);
        byte[] buf;
        
        bm.clear();
        bm.set(1, true);  // 0x 00 00 00 01
        buf = bm.toBytes();
        Assert.assertEquals(4, buf.length);
        Assert.assertEquals(2, buf[0]);
        Assert.assertEquals(0, buf[1]);
        Assert.assertEquals(0, buf[2]);
        Assert.assertEquals(0, buf[3]);
        
        bm.clear();
        bm.set(7, true); // 0x 00 00 00 80
        buf = bm.toBytes();
        Assert.assertEquals(4, buf.length);
        Assert.assertEquals(128, buf[0] & 0x000000ff);
        Assert.assertEquals(0, buf[1]);
        Assert.assertEquals(0, buf[2]);
        Assert.assertEquals(0, buf[3]);
        
        bm.clear();
        bm.set(8, true); // 0x 00 00 01 00
        buf = bm.toBytes();
        Assert.assertEquals(4, buf.length);
        Assert.assertEquals(buf[0], 0);
        Assert.assertEquals(buf[1], 1);
        Assert.assertEquals(buf[2], 0);
        Assert.assertEquals(buf[3], 0);
        
        bm.clear();
        bm.set(16, true); // 0x 00 01 00 00
        buf = bm.toBytes();
        Assert.assertEquals(4, buf.length);
        Assert.assertEquals(buf[0], 0);
        Assert.assertEquals(buf[1], 0);
        Assert.assertEquals(buf[2], 1);
        Assert.assertEquals(buf[3], 0);
        
        // what about a buf that spans two chunks?
        
        buf = bm.toBytes(1, 2);
        Assert.assertEquals(2, buf.length);
        Assert.assertEquals(0, buf[0]);
        Assert.assertEquals(1, buf[1]);
        
        buf = bm.toBytes(2, 2);
        Assert.assertEquals(2, buf.length);
        Assert.assertEquals(1, buf[0]);
        Assert.assertEquals(0, buf[1]);
    }
}
