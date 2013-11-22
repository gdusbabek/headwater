package headwater.cassandra;

import headwater.data.CassandraIO;
import headwater.data.IO;
import headwater.data.MemoryKeyObserver;
import headwater.text.AbstractTrigramIndexTest;
import headwater.text.CTrigramIndex;
import headwater.text.ITrigramIndex;

public class IntegrationTestCassandraTrigramIndex extends AbstractTrigramIndexTest {

    @Override
    public ITrigramIndex<String, String> makeIndex() {
        
        MemoryKeyObserver<String, String, String> dataAccess = new MemoryKeyObserver<String, String, String>();
        IO io = new CassandraIO("127.0.0.1", 9160, "headwater", "my_data_trigram_index");
        return new CTrigramIndex<String, String>(1073741824L, 4194304)
                        .withIO(io)
                        .withObserver(dataAccess)
                        .withLookup(dataAccess);
    }
}
