package headwater.data;

import headwater.data.AbstractIOTest;
import headwater.data.FakeCassandraIO;

public class TestFakeCassandraIO extends AbstractIOTest {

    @Override
    public void createIO() throws Exception {
        io = new FakeCassandraIO();
    }
}
