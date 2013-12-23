package headwater.cassandra;

import com.netflix.astyanax.connectionpool.exceptions.OperationTimeoutException;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import headwater.data.CassSerializers;
import headwater.data.CassandraIO;
import headwater.data.FakeCassandraIO;
import headwater.data.IO;
import headwater.data.IOLookupObserver;
import headwater.data.KeyObserver;
import headwater.data.Lookup;
import headwater.data.MemLookupObserver;
import headwater.hash.BitHashableKey;
import headwater.hash.Hashers;
import headwater.text.AbstractTrigramIndexTest;
import headwater.text.CTrigramIndex;
import headwater.text.ITrigramIndex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

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
