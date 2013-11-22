package headwater.bitmap;

import headwater.cassandra.CBitmap;
import headwater.cassandra.IO;
import org.junit.Test;

public class TestCBitmap extends AbstractBitmapTest{
    
    private IO io;
    
    @Override
    public void instantiate() {
        io = new FakeCassandraIO();
        x = new CBitmap("x".getBytes(), 16, 8, io);
        y = new CBitmap("y".getBytes(), 16, 8, io);
        wide = new CBitmap("wide".getBytes(), 32, 16, io);
        padded = new CBitmap("padded".getBytes(), 16, 16, io);
        setZero = new CBitmap("setZero".getBytes(), 32, 8, io);
    }
}
