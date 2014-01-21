package headwater.bitmap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class TestBitmaps {
    
    private final IBitmap x;
    private final IBitmap y;
    private final IBitmap wide;
    private final IBitmap padded;
    private final IBitmap setZero;
    private final IBitmap bm0, bm1;
    
    public TestBitmaps(IBitmap x, IBitmap y, IBitmap wide, IBitmap padded, IBitmap setZero, IBitmap bm0, IBitmap bm1) {
        this.x = x;
        this.y = y;
        this.wide = wide;
        this.padded = padded;
        this.setZero = setZero;
        this.bm0 = bm0;
        this.bm1 = bm1;
    }
    
    @Before
    public void init() {
        // both maps have the 3 bit asserted.
        x.set(0, true);
        x.set(3, true);
        y.set(7, true);
        y.set(3, true);
        
        wide.set(0, true);
        wide.set(1, true);
        wide.set(31, true);
        
        padded.set(1, true);
        
        setZero.set(0, true);
    }
    
    @Test
    public void testEmptyArrayIsAllZeros() {
        byte[] buffer = new byte[10000];
        Assert.assertEquals(10000, buffer.length);
        for (byte b : buffer)
            Assert.assertEquals(0, b);
    }
    
    @Test
    public void testNewBuffsAreZeroed() {
        byte[] buf = new byte[0x00100000];
        for (byte b : buf)
            Assert.assertEquals(0, b);
    }
    
    @Test
    public void testGetAsserted() {
        Assert.assertTrue(arrayEquals(new long[]{0, 3}, x.getAsserted()));
        Assert.assertTrue(arrayEquals(new long[]{3, 7}, y.getAsserted()));
        Assert.assertTrue(arrayEquals(new long[]{0, 1, 31}, wide.getAsserted()));
    }
    
    @Test
    public void testAndNoMutate() {
        IBitmap and = BitmapUtils.nonMutatingAND(x, y);
        
        Assert.assertTrue(arrayEquals(new long[]{0, 3}, x.getAsserted()));
        Assert.assertTrue(arrayEquals(new long[]{3, 7}, y.getAsserted()));
        Assert.assertTrue(arrayEquals(new long[]{3}, and.getAsserted()));
    }
    
    @Test
    public void testOrNoMutate() {
        IBitmap or = BitmapUtils.nonMutatingOR(x, y);
        
        Assert.assertTrue(arrayEquals(new long[]{0, 3}, x.getAsserted()));
        Assert.assertTrue(arrayEquals(new long[]{3, 7}, y.getAsserted()));
        // order isn't important, so the expected should be randomized and we should do a set compares.
        Assert.assertTrue(arrayEquals(new long[]{0, 3, 7}, or.getAsserted()));
    }
    
    // least significant bytes first.
    @Test
    public void testByteArray() {
        byte[] buf = wide.toBytes();
        Assert.assertEquals(4, buf.length);
        Assert.assertEquals(3, buf[0]);
        Assert.assertEquals(0, buf[1]);
        Assert.assertEquals(0, buf[2]);
        Assert.assertEquals(0x80, buf[3] & 0x000000ff);
    }
    
    @Test
    public void testChunkingLoEnd() {
        byte[] chunk = wide.toBytes(0, 2);
        Assert.assertEquals(2, chunk.length);
        // least significant byte is first.
        Assert.assertEquals(3, chunk[0]);
        Assert.assertEquals(0, chunk[1]);
    }
    
    @Test
    public void testChunkingHiEnd() {
        byte[] chunk = wide.toBytes(2, 2);
        Assert.assertEquals(2, chunk.length);
        Assert.assertEquals(0, chunk[0]);
        Assert.assertEquals(0x80, chunk[1] & 0x000000ff);
    }
    
    @Test
    public void testPaddingIsOnCorrectEnd() {
        // the correct end is the left end of the array. 
        Assert.assertEquals(2, padded.toBytes().length);
        Assert.assertEquals(2, padded.toBytes()[0]);
        Assert.assertEquals(0, padded.toBytes()[1]);
    }
    
    @Test
    public void setBitSetAssertionsFixed() {
        byte[] buf;
        
        wide.clear();
        wide.set(1, true);  // 0x 00 00 00 01
        buf = wide.toBytes();
        Assert.assertEquals(4, buf.length);
        Assert.assertEquals(2, buf[0]);
        Assert.assertEquals(0, buf[1]);
        Assert.assertEquals(0, buf[2]);
        Assert.assertEquals(0, buf[3]);
        
        wide.clear();
        wide.set(7, true); // 0x 00 00 00 80
        buf = wide.toBytes();
        Assert.assertEquals(4, buf.length);
        Assert.assertEquals(128, buf[0] & 0x000000ff);
        Assert.assertEquals(0, buf[1]);
        Assert.assertEquals(0, buf[2]);
        Assert.assertEquals(0, buf[3]);
        
        wide.clear();
        wide.set(8, true); // 0x 00 00 01 00
        buf = wide.toBytes();
        Assert.assertEquals(4, buf.length);
        Assert.assertEquals(buf[0], 0);
        Assert.assertEquals(buf[1], 1);
        Assert.assertEquals(buf[2], 0);
        Assert.assertEquals(buf[3], 0);
        
        wide.clear();
        wide.set(16, true); // 0x 00 01 00 00
        buf = wide.toBytes();
        Assert.assertEquals(4, buf.length);
        Assert.assertEquals(buf[0], 0);
        Assert.assertEquals(buf[1], 0);
        Assert.assertEquals(buf[2], 1);
        Assert.assertEquals(buf[3], 0);
        
    }
    
    @Test
    public void testIsEmpty() {
        Assert.assertFalse(x.isEmpty());
        Assert.assertFalse(y.isEmpty());
        Assert.assertFalse(wide.isEmpty());
        Assert.assertFalse(padded.isEmpty());
        
        x.clear();
        y.clear();
        wide.clear();
        padded.clear();
        
        Assert.assertTrue(x.isEmpty());
        Assert.assertTrue(y.isEmpty());
        Assert.assertTrue(wide.isEmpty());
        Assert.assertTrue(padded.isEmpty());
    }
    
    @Test
    public void testByteAndBitEndian() {
        
        Assert.assertEquals(1, setZero.toBytes()[0]);
        Assert.assertEquals(0, setZero.toBytes()[1]);
        Assert.assertEquals(0, setZero.toBytes()[2]);
        Assert.assertEquals(0, setZero.toBytes()[3]);
        
        setZero.clear();
        setZero.set(3, true);
        
        Assert.assertEquals(8, setZero.toBytes()[0]);
        Assert.assertEquals(0, setZero.toBytes()[1]);
        Assert.assertEquals(0, setZero.toBytes()[2]);
        Assert.assertEquals(0, setZero.toBytes()[3]);
        
        setZero.clear();
        setZero.set(8, true);
        
        Assert.assertEquals(0, setZero.toBytes()[0]);
        Assert.assertEquals(1, setZero.toBytes()[1]);
        Assert.assertEquals(0, setZero.toBytes()[2]);
        Assert.assertEquals(0, setZero.toBytes()[3]);  
    }
    
    public static boolean arrayEquals(long[] x, long[] y) {
        if (x.length != y.length) return false;
        for (int i = 0; i < x.length; i++)
            if (x[i] != y[i])
                return false;
        return true;
            
    }
    
    @Test
    public void testMemoryBitmapSetAll() {
        Assert.assertNotEquals(bm0, bm1);
        Assert.assertEquals(1, bm0.getAsserted().length);
        Assert.assertEquals(0, bm1.getAsserted().length);
        
        bm1.setAll(bm0.toBytes());
        
        Assert.assertEquals(1, bm0.getAsserted().length);
        Assert.assertEquals(1, bm1.getAsserted().length);
        Assert.assertEquals(bm0, bm1);
    }
    
    @Parameterized.Parameters
    public static List<Object[]> getParameters() {
        return new ArrayList<Object[]>() {{
            add(buildMemoryBitmapInputs());
            add(buildMemoryBitmap2Inputs());
        }};
    }
    
    private static Object[] buildMemoryBitmapInputs() {
        return new Object[] {
                new MemoryBitmap(16), // x
                new MemoryBitmap(16), // y
                new MemoryBitmap(32), // wide
                new MemoryBitmap(16), // padded
                new MemoryBitmap(32), // setZero
                MemoryBitmap.wrap(new byte[]{0,0,0,0,1}), // bm0
                MemoryBitmap.wrap(new byte[]{0,0,0,0,0}) // bm1
        };
    }
    
    private static Object[] buildMemoryBitmap2Inputs() {
        return new Object[] {
                new MemoryBitmap2(16), // x
                new MemoryBitmap2(16), // y
                new MemoryBitmap2(32), // wide
                new MemoryBitmap2(16), // padded
                new MemoryBitmap2(32), // setZero
                MemoryBitmap2.wrap(new byte[]{0,0,0,0,1}), // bm0
                MemoryBitmap2.wrap(new byte[]{0,0,0,0,0}) // bm1
        };
    }
}
