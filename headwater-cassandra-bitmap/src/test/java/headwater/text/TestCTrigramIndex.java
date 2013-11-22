package headwater.text;

import headwater.bitmap.FakeCassandraIO;
import headwater.cassandra.IO;
import headwater.data.KeyObserver;
import headwater.data.MemoryKeyObserver;

public class TestCTrigramIndex extends AbstractTrigramIndexTest {
    
    @Override
    public ITrigramIndex<String, String> makeIndex() {
        
        MemoryKeyObserver<String, String, String> dataAccess = new MemoryKeyObserver<String, String, String>();
        IO io = new FakeCassandraIO();
        // a 1Gbit index with 4Mbit segments.
        return new CTrigramIndex<String, String>(1073741824L, 4194304)
                .withIO(io)
                .withObserver(dataAccess)
                .withLookup(dataAccess);
    }
}
