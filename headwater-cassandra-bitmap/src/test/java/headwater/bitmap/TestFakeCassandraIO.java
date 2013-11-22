package headwater.bitmap;

import headwater.cassandra.AbstractIOTest;

public class TestFakeCassandraIO extends AbstractIOTest {

    @Override
    public void createIO() throws Exception {
        io = new FakeCassandraIO();
    }
}
