package headwater.bitmap;

import headwater.data.FakeCassandraIO;
import headwater.data.IO;

public class TestIOBitmap extends AbstractBitmapTest{
    
    private IO io;
    
    @Override
    public void instantiate() {
        io = new FakeCassandraIO();
        x = new IOBitmap("x".getBytes(), 16, 8, io);
        y = new IOBitmap("y".getBytes(), 16, 8, io);
        wide = new IOBitmap("wide".getBytes(), 32, 16, io);
        padded = new IOBitmap("padded".getBytes(), 16, 16, io);
        setZero = new IOBitmap("setZero".getBytes(), 32, 8, io);
    }
}
