package headwater.cassandra;

import headwater.bitmap.AbstractBitmapTest;
import headwater.data.IO;

public class TestIntegrationCBitmap extends AbstractBitmapTest {
    
    private IO io;
    
    @Override
    public void instantiate() {
        io = new CassandraIO("127.0.0.1", 9160, "headwater", "my_data_index");
        x = new CBitmap("x".getBytes(), 16, 8, io);
        y = new CBitmap("y".getBytes(), 16, 8, io);
        wide = new CBitmap("wide".getBytes(), 32, 16, io);
        padded = new CBitmap("padded".getBytes(), 16, 16, io);
        setZero = new CBitmap("setZero".getBytes(), 32, 8, io);
    }
}
