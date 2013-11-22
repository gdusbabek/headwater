package headwater.bitmap;

import junit.framework.Assert;
import org.junit.Test;

public class TestAssumptions {
    @Test
    public void testEmptyArrayIsAllZeros() {
        byte[] buffer = new byte[10000];
        Assert.assertEquals(10000, buffer.length);
        for (byte b : buffer)
            Assert.assertEquals(0, b);
    }
}
