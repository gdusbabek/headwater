package headwater.io;

import headwater.Utils;
import headwater.bitmap.IBitmap;
import headwater.bitmap.MemoryBitmap2;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class TestIO {
    private final IO<Long, IBitmap> io;
    
    public TestIO(IO<Long, IBitmap> io) {
        this.io = io;
    }
    
    @Test
    public void testReadWrite() throws Exception {
        byte[] key = "testReadWrite".getBytes();
        long col = 454322353452342L;
        IBitmap value = MemoryBitmap2.wrap(new byte[] {8, 9, 10, 11, 12});
        
        io.put(key, col, value);
        
        IBitmap readValue = io.get(key, col);

        Assert.assertNotNull(readValue);
        assertArraysEqual(value.toBytes(), readValue.toBytes());
    }
    
    @Test
    public void testRowReadIteration() throws Exception {
        byte[] key = "testRowReadIteration".getBytes();
        int numCols = 10000;
        for (int i = 0; i < numCols; i++)
            io.put(key, (long)i, MemoryBitmap2.wrap(Utils.longToBytes(1000L * i)));
        
        final AtomicInteger readCount = new AtomicInteger(0);
        io.visitAllColumns(key, 2, new ColumnObserver<Long, IBitmap>() {
            public void observe(byte[] row, Long col, IBitmap value) {
                readCount.incrementAndGet();  
            }
        });
                
        Assert.assertEquals(numCols, readCount.get());
            
    }
    
    private static void assertArraysEqual(byte[] expected, byte[] actual) {
        Assert.assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++)
            Assert.assertEquals(expected[i], actual[i]);
    }
    
    @Parameterized.Parameters
    public static List<Object[]> getParameters() {
        return new ArrayList<Object[]>() {{
            add(new Object[]{new MemoryBitmapIO()});
        }};
    }
}
