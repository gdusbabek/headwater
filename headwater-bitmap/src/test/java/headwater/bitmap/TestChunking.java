package headwater.bitmap;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestChunking {
    
    private JuBitmap bm;
    
    @Before
    public void init() {
        bm = new JuBitmap(128);
    }
    
    @Test
    public void testDividingEmptyBitsNoSkip() {
        List<Chunk> chunkList = BitmapChunker.divide(bm, 2, false);
        // 128 bits is 16 bytes is 8 chunks.
        Assert.assertEquals(8, chunkList.size());
        for (Chunk ch : chunkList) {
            Assert.assertTrue(ch.getValues().length > 0);
            for (byte b : ch.getValues()) {
                Assert.assertEquals(0, b);
            }
        }
    }
    
    @Test
    public void testDividingEmptyBitsWithSkip() {
        Assert.assertEquals(0, BitmapChunker.divide(bm, 2, true).size());
    }
    
    @Test
    public void testDividingSparseMapNoSkip() {
        bm.set(0, true);
        bm.set(127, true);
        List<Chunk> chunkList = BitmapChunker.divide(bm, 2, false);
        Assert.assertEquals(8, chunkList.size());
        
        // test first and last.
        Chunk lo = chunkList.remove(0);
        Chunk hi = chunkList.remove(chunkList.size() - 1);
        
        Assert.assertEquals(0, lo.getOffset());
        Assert.assertEquals(1, lo.getValues()[0]);
        Assert.assertEquals(0, lo.getValues()[1]);
        
        Assert.assertEquals(7, hi.getOffset());
        Assert.assertEquals(0, hi.getValues()[0]);
        Assert.assertEquals(128, hi.getValues()[1] & 0x000000ff);
        
        // all the rest should be zeros.
        for (Chunk ch : chunkList) {
            Assert.assertTrue(ch.getValues().length > 0);
            for (byte b : ch.getValues()) {
                Assert.assertEquals(0, b);
            }
        }
        
    }
    
    @Test
    public void testDividingSparseMapWithSkip() {
        bm.set(0, true);
        bm.set(127, true);
        List<Chunk> chunkList = BitmapChunker.divide(bm, 2, true);
        // there should be 6 empty chunks.
        Assert.assertEquals(2, chunkList.size());
        
        Chunk lo = chunkList.remove(0);
        Chunk hi = chunkList.remove(0);
        
        Assert.assertEquals(0, lo.getOffset());
        Assert.assertEquals(1, lo.getValues()[0]);
        Assert.assertEquals(0, lo.getValues()[1]);
        
        Assert.assertEquals(7, hi.getOffset());
        Assert.assertEquals(0, hi.getValues()[0]);
        Assert.assertEquals(128, hi.getValues()[1] & 0x000000ff);
    }
}
