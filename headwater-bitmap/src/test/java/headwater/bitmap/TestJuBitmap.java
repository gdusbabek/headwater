package headwater.bitmap;

import junit.framework.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.BitSet;

public class TestJuBitmap extends AbstractBitmapTest {
    
    
    public void instantiate() {
        x = new JuBitmap(16);
        y = new JuBitmap(16);
        wide = new JuBitmap(32);
        padded = new JuBitmap(16);
        setZero = new JuBitmap(32);
    }
    
    @Test
    public void bitties() {
        byte x = 1;
        for (int i = 0; i < 8; i++) {
            x <<= 1;
        }
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalValues() {
        new JuBitmap(27);
    }
    
    @Test
    public void testEmptyChunkGetPadding() {
        JuBitmap bm = new JuBitmap(16);
        Assert.assertEquals(2, bm.toBytes().length);
    }
    
    @Test
    public void testBitSetAssuptions() {
        BitSet assumptions;
        byte[] buf;
        
        assumptions = new BitSet(32);
        assumptions.set(1, true); // 0x 00 00 00 01
        buf = assumptions.toByteArray();
        Assert.assertEquals(1, buf.length);
        Assert.assertEquals(2, buf[0]);
        
        assumptions = new BitSet(32);
        assumptions.set(7, true); // 0x 00 00 00 80
        buf = assumptions.toByteArray();
        Assert.assertEquals(1, buf.length);
        Assert.assertEquals(128, buf[0] & 0x000000ff);
        
        assumptions = new BitSet(32);
        assumptions.set(8, true); // 0x 00 00 01 00
        buf = assumptions.toByteArray();
        Assert.assertEquals(2, buf.length);
        Assert.assertEquals(buf[0], 0);
        Assert.assertEquals(buf[1], 1);
        
        assumptions = new BitSet(32);
        assumptions.set(16, true); // 0x 00 01 00 00
        buf = assumptions.toByteArray();
        Assert.assertEquals(3, buf.length);
        Assert.assertEquals(buf[0], 0);
        Assert.assertEquals(buf[1], 0);
        Assert.assertEquals(buf[2], 1);
        
        // at this point, it becomes clear that we are getting the least significant bytes first.
        // so if we need to pad on the conceptual left (beginning) of this buffer.
    }
}
