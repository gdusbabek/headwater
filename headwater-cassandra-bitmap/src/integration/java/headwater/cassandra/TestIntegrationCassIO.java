package headwater.cassandra;

import junit.framework.Assert;
import org.junit.Test;

public class TestIntegrationCassIO {
    
    @Test
    public void testReadWrite() throws Exception {
        byte[] key = new byte[] {1,2,3};
        byte[] col = new byte[] {4, 5, 6, 7};
        byte[] value = new byte[] {8, 9, 10, 11, 12};
        
        CassandraIO io = new CassandraIO("127.0.0.1", 9160, "headwater", "my_data_index");
        io.put(key, col, value);
        
        byte[] readValue = io.get(key, col);

        Assert.assertTrue(readValue.length > 0);
        assertArraysEqual(value, readValue);
    }
    
    private static void assertArraysEqual(byte[] expected, byte[] actual) {
        Assert.assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++)
            Assert.assertEquals(expected[i], actual[i]);
    }
}
