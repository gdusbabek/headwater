package headwater.bitmap;

import junit.framework.Assert;
import org.junit.Test;

import java.util.BitSet;

public class TestBitsets {
    
    @Test
    public void testPopArray() {
        long[] buf = new long[]{
                0x00000000000001,
                0x00000000000002,
                0x00000000000004,
                0x00000000000008,
                0x00000000000010,
                0x00000000000020,
                0x00000000000040,
                0x00000000000000, // no bits asserted!
                
                0x00000000000080,
                0x00000000000100,
                0x00000000000200,
                0x00000000000400,
                0x00000000000800,
                0x00000000001000,
                0x00000000000000, // no bits asserted.
                0x00000000002000,
        };
        
        long expected = 14;
        long actual = BitUtil.pop_array(buf, 0, buf.length);
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testJavaBitsetCardinality() {
        BitSet bs = new BitSet(0x00100000);
        bs.set(89123);
        Assert.assertEquals(1, bs.cardinality());
    }
    
    @Test
    public void testOpenBitsetCardinality() {
        OpenBitSet bs = new OpenBitSet(0x00100000);
        bs.set(89123);
        Assert.assertEquals(1, bs.cardinality());
    }
}
