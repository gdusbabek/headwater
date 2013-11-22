package headwater.data;

import headwater.util.Utils;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

@Ignore
public abstract class AbstractIOTest {
    
    protected IO io;
    
    @Before
    public abstract void createIO() throws Exception ;
    
    @Test
    public void testReadWrite() throws Exception {
        byte[] key = "testReadWrite".getBytes();
        byte[] col = new byte[] {4, 5, 6, 7};
        byte[] value = new byte[] {8, 9, 10, 11, 12};
        
        io.put(key, col, value);
        
        byte[] readValue = io.get(key, col);

        Assert.assertTrue(readValue.length > 0);
        assertArraysEqual(value, readValue);
    }
    
    @Test
    public void testRowReadIteration() throws Exception {
        byte[] key = "testRowReadIteration".getBytes();
        int numCols = 10000;
        for (int i = 0; i < numCols; i++)
            io.put(key, Utils.longToBytes((long) i), Utils.longToBytes(1000L * i));
        
        final AtomicInteger readCount = new AtomicInteger(0);
        io.visitAllColumns(key, 2, new ColumnObserver() {
            public void observe(byte[] row, byte[] col, byte[] value) {
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
}
