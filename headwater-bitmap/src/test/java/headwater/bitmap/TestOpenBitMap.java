package headwater.bitmap;


import junit.framework.Assert;
import org.junit.Test;

import java.util.BitSet;

public class TestOpenBitMap extends AbstractBitmapTest {
    
    @Override
    public void instantiate() {
        x = new OpenBitmap(16);
        y = new OpenBitmap(16);
        wide = new OpenBitmap(32);
        padded = new OpenBitmap(16);
        setZero = new OpenBitmap(32);
    }
    
    @Test
    public void testSetZerosAreSameBinary() {
        OpenBitSet obs = new OpenBitSet(32);
        BitSet bs = new BitSet(32);
        
        obs.set(0);
        bs.set(0);
        
        byte[] bobs = obs.toByteArray();
        byte[] bbs = bs.toByteArray();
        
        Assert.assertEquals(bobs.length, bbs.length);
    }
    
    @Test
    public void testBinaryNatures() {
        int bits = 0x00100000;
        OpenBitSet obs = new OpenBitSet(bits);
        BitSet bs = new BitSet(bits);
        
        obs.set(17);
        
        bs.set(17);
        
        byte[] bufObs = obs.toByteArray();
        byte[] bufBs = bs.toByteArray();
        
        Assert.assertEquals(bufObs.length, bufBs.length);
        
    }
    
    @Test
   public void testBitSetAssuptions() {
       OpenBitSet assumptions;
       byte[] buf;
       
       assumptions = new OpenBitSet(32);
       assumptions.set(1); // 0x 00 00 00 01
       buf = assumptions.toByteArray();
       Assert.assertEquals(1, buf.length);
       Assert.assertEquals(2, buf[0]);
       
       assumptions = new OpenBitSet(32);
       assumptions.set(7); // 0x 00 00 00 80
       buf = assumptions.toByteArray();
       Assert.assertEquals(1, buf.length);
       Assert.assertEquals(128, buf[0] & 0x000000ff);
       
       assumptions = new OpenBitSet(32);
       assumptions.set(8); // 0x 00 00 01 00
       buf = assumptions.toByteArray();
       Assert.assertEquals(2, buf.length);
       Assert.assertEquals(buf[0], 0);
       Assert.assertEquals(buf[1], 1);
       
       assumptions = new OpenBitSet(32);
       assumptions.set(16); // 0x 00 01 00 00
       buf = assumptions.toByteArray();
       Assert.assertEquals(3, buf.length);
       Assert.assertEquals(buf[0], 0);
       Assert.assertEquals(buf[1], 0);
       Assert.assertEquals(buf[2], 1);
       
       // at this point, it becomes clear that we are getting the least significant bytes first.
       // so if we need to pad on the conceptual left (beginning) of this buffer.
   }
}
