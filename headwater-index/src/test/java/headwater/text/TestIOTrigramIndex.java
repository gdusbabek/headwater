package headwater.text;

import headwater.data.FakeCassandraIO;
import headwater.data.IO;
import headwater.data.MemLookupObserver;

public class TestIOTrigramIndex extends AbstractTrigramIndexTest {
    
    @Override
    public ITrigramIndex<String, String> makeIndex() {
        
        MemLookupObserver<String, String, String> dataAccess = new MemLookupObserver<String, String, String>();
        IO io = new FakeCassandraIO();
        // a 1Gbit index with 4Mbit segments.
        return new CTrigramIndex<String, String>(1073741824L, 4194304)
                .withIO(io)
                .withObserver(dataAccess)
                .withLookup(dataAccess);
    }
}
