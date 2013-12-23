package headwater.cassandra;

import com.netflix.astyanax.serializers.StringSerializer;
import headwater.data.CassandraIO;
import headwater.data.IO;
import headwater.data.IOLookupObserver;

import headwater.text.AbstractTrigramIndexTest;
import headwater.text.CTrigramIndex;


public class IntegrationTestCassandraTrigramIndex extends AbstractTrigramIndexTest {

    @Override
    public void setReaderAndWriter() {
        
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
        CTrigramIndex<String, String> index =  new CTrigramIndex<String, String>(1073741824L, 4194304)
                        .withIO(io)
                        .withObserver(dataAccess)
                        .withLookup(dataAccess);
        this.reader = index;
        this.writer = index;
    }

    public static void main(String args[]) {
        
        if (System.getProperty("SHAKESPEARE_PATH") == null)
            throw new RuntimeException("Please set SHAKESPEARE_PATH");
        
        IO io = new CassandraIO("127.0.0.1", 9160, "headwater", "my_data_trigram_index");
        try {
            new IntegrationTestCassandraTrigramIndex().buildIndex(io);
            new IntegrationTestCassandraTrigramIndex().queryIndex(io);
        } catch (Throwable th) {
            th.printStackTrace();
            System.exit(-1);
        }
    }
}
