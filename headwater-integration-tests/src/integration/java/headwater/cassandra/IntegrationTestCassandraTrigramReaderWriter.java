package headwater.cassandra;

import com.netflix.astyanax.serializers.StringSerializer;
import headwater.data.CassandraIO;
import headwater.data.IO;
import headwater.data.IOLookupObserver;

import headwater.text.AbstractTrigramReaderWriterTest;
import headwater.text.BareIOTrigramReader;
import headwater.text.BareIOTrigramWriter;


public class IntegrationTestCassandraTrigramReaderWriter extends AbstractTrigramReaderWriterTest {

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
        this.reader =  new BareIOTrigramReader<String, String>(1073741824L, 4194304)
                        .withIO(io)
                        .withLookup(dataAccess);
        this.writer = new BareIOTrigramWriter<String, String>(1073741824L, 4194304)
                .withIO(io)
                .withObserver(dataAccess);
    }

    public static void main(String args[]) {
        
        if (System.getProperty("SHAKESPEARE_PATH") == null)
            throw new RuntimeException("Please set SHAKESPEARE_PATH");
        
        IO io = new CassandraIO("127.0.0.1", 9160, "headwater", "my_data_trigram_index");
        try {
            new IntegrationTestCassandraTrigramReaderWriter().buildIndex(io);
            new IntegrationTestCassandraTrigramReaderWriter().queryIndex(io);
        } catch (Throwable th) {
            th.printStackTrace();
            System.exit(-1);
        }
    }
}
