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
import headwater.data.MemLookupObserver;
import headwater.hash.BitHashableKey;
import headwater.text.AbstractTrigramIndexTest;
import headwater.text.CTrigramIndex;
import headwater.text.ITrigramIndex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Collection;
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
    
    
    // CASSANDRA_HOME=/Users/gdusbabek/Downloads/apache-cassandra-1.2.9 /Users/gdusbabek/Downloads/apache-cassandra-cli < /Users/gdusbabek/codes/github/headwater/headwater-integration-tests/src/integration/resources/load.script
    private void testPerformance() throws Exception {
        
        MemLookupObserver<String, String, String> keyObserver = new MemLookupObserver<String, String, String>();
        IO io = new CassandraIO("127.0.0.1", 9160, "headwater", "my_data_trigram_index");
//        CTrigramIndex<String, String> index = new CTrigramIndex<String, String>(1073741824L, 4194304)
        CTrigramIndex<String, String> index = new CTrigramIndex<String, String>(2097152L, 65536)
                .withIO(io)
                .withObserver(keyObserver)
                .withLookup(keyObserver);

        File shakespeareDir = new File(System.getProperty("SHAKESPEARE_PATH"));
        File[] listing = shakespeareDir.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                if (pathname.isHidden())
                    return false;
                if (pathname.getName().equals("glossary"))
                    return false;
                return true;
            }
        });
        
        final AtomicInteger lineCounter = new AtomicInteger(0);
        
//        new Thread("Dumper") { public void run() {
//            while (true) {
//                try { sleep(30000); } catch (Exception e) {};
//                dumpMetrics();
//            }
//        }}.start();
        
        for (File file : listing) {
            System.out.println("indexing " + file.getName());
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line = reader.readLine();
            int lineNumber = 0;
            while (line != null) {
                line = line.trim();
                if (line.length() > 0) {
                    try {
                        index.add(file.getName() + "_" + lineNumber, "TEXT", line);
                    } catch (Throwable th) {
                        System.out.println("OUCH " + th.getMessage());
                        if (th.getCause() != null && th.getCause() instanceof OperationTimeoutException) {
                            System.out.println("Pausing");
                            Thread.sleep(5000);
                        }
                        int numRetries = 5;
                        for (int i = 0; i < numRetries; i++) {
                            try {
                                io = new CassandraIO("127.0.0.1", 9160, "headwater", "my_data_trigram_index");
                                index = index.withIO(io);
                                index.add(file.getName() + "_" + lineNumber, "TEXT", line);
                                System.out.println("retry success");
                                break;
                            } catch (Throwable ouch) {
                                System.out.println("retry failure");
                                Thread.sleep(500);
                            }
                        }
                    }
                }
                lineNumber += 1;
                if (lineNumber % 500 == 0) {
                    System.out.println(lineNumber);
                }
                line = reader.readLine();
                lineCounter.incrementAndGet();
            }
        }
    }
    
    private static void dumpMetrics() {
        MetricsRegistry registry = Metrics.defaultRegistry();
        
        SortedMap<String, SortedMap<MetricName, Metric>> metricMap = registry.groupedMetrics(new MetricPredicate() {
            public boolean matches(MetricName name, Metric metric) {
                return name.getScope().equals("trigram");
            }
        });
        
        SortedMap<MetricName, Metric> metrics = metricMap.values().iterator().next();
        for (Map.Entry<MetricName, Metric> entry : metrics.entrySet()) {
            System.out.println(entry.getKey().getName());
            if (entry.getValue() instanceof Timer) {
                Timer timer = (Timer)entry.getValue();
                Snapshot snapshot = timer.getSnapshot();
                System.out.println(String.format("  98th: %s", snapshot.get98thPercentile()));
            }
        }
    }
    
    public static void main(String args[]) {
        
        if (System.getProperty("SHAKESPEARE_PATH") == null)
            throw new RuntimeException("Please set SHAKESPEARE_PATH");
        
        try {
            new IntegrationTestCassandraTrigramIndex().testPerformance();
        } catch (Throwable th) {
            th.printStackTrace();
            System.exit(-1);
        }
    }
}
