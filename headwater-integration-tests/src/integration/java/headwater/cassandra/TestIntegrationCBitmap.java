package headwater.cassandra;

import headwater.bitmap.AbstractBitmapTest;
import headwater.bitmap.IOBitmap;
import headwater.data.IO;
import headwater.data.CassandraIO;

public class TestIntegrationCBitmap extends AbstractBitmapTest {
    
    private IO io;
    
    @Override
    public void instantiate() {
        io = new CassandraIO("127.0.0.1", 9160, "headwater", "my_data_index");
        x = new IOBitmap("x".getBytes(), 16, 8, io);
        y = new IOBitmap("y".getBytes(), 16, 8, io);
        wide = new IOBitmap("wide".getBytes(), 32, 16, io);
        padded = new IOBitmap("padded".getBytes(), 16, 16, io);
        setZero = new IOBitmap("setZero".getBytes(), 32, 8, io);
    }
}
