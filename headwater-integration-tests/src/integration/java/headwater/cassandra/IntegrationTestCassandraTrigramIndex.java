package headwater.cassandra;

import com.netflix.astyanax.serializers.StringSerializer;
import headwater.data.CassandraIO;
import headwater.data.FakeCassandraIO;
import headwater.data.IO;
import headwater.data.IOLookupObserver;
import headwater.text.AbstractTrigramIndexTest;
import headwater.text.CTrigramIndex;
import headwater.text.ITrigramIndex;

public class IntegrationTestCassandraTrigramIndex extends AbstractTrigramIndexTest {

    @Override
    public ITrigramIndex<String, String> makeIndex() {
        
        IOLookupObserver<String, String, String> dataAccess = new IOLookupObserver<String, String, String>(
                new CassandraIO("127.0.0.1", 9160, "headwater", "my_lookup_data"),
                new CassandraIO("127.0.0.1", 9160, "headwater", "my_bit_observer"),
//                new FakeCassandraIO(),
//                new FakeCassandraIO(),
                false,
                StringSerializer.get(),
                StringSerializer.get(),
                StringSerializer.get()
        );
        IO io = new CassandraIO("127.0.0.1", 9160, "headwater", "my_data_trigram_index");
//        IO io = new FakeCassandraIO();
        return new CTrigramIndex<String, String>(1073741824L, 4194304)
                        .withIO(io)
                        .withObserver(dataAccess)
                        .withLookup(dataAccess);
    }
}
