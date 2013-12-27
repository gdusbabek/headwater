package headwater.text;

import headwater.data.FakeCassandraIO;
import headwater.data.IO;
import headwater.data.MemLookupObserver;

public class TestIOTrigramReaderWriter extends AbstractTrigramReaderWriterTest {

    @Override
    public void setReaderAndWriter() {
        MemLookupObserver<String, String, String> dataAccess = new MemLookupObserver<String, String, String>();
        IO io = new FakeCassandraIO();
        // a 1Gbit index with 4Mbit segments.
        // those are huge segments fwiw.
        this.reader = new BareIOTrigramReader<String, String>(1073741824L, 4194304)
                .withIO(io)
                .withLookup(dataAccess);
        this.writer = new BareIOTrigramWriter<String, String>(1073741824L, 4194304)
                .withIO(io)
                .withObserver(dataAccess);
    }

    
    public static void main(String args[]) {        
        if (System.getProperty("SHAKESPEARE_PATH") == null)
            throw new RuntimeException("Please set SHAKESPEARE_PATH");
        
        IO io = new FakeCassandraIO();
        try {
            new TestIOTrigramReaderWriter().buildIndex(io);
            new TestIOTrigramReaderWriter().queryIndex(io);
        } catch (Throwable th) {
            th.printStackTrace();
            System.exit(-1);
        }
    }
}
