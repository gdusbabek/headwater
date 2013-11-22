package headwater.bitmap;

import headwater.data.AbstractIOTest;

public class TestFakeCassandraIO extends AbstractIOTest {

    @Override
    public void createIO() throws Exception {
        io = new FakeCassandraIO();
    }
}
