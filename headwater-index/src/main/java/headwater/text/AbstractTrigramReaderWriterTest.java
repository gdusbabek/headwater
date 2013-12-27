package headwater.text;


import com.google.common.collect.Sets;
import com.netflix.astyanax.connectionpool.exceptions.OperationTimeoutException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import headwater.data.IO;
import headwater.data.Lookup;
import headwater.data.MemLookupObserver;
import headwater.hash.BitHashableKey;
import headwater.hash.Hashers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

public abstract class AbstractTrigramReaderWriterTest {
    
    
    protected ITrigramReader<String, String> reader;
    protected ITrigramWriter<String, String> writer;
    
    public abstract void setReaderAndWriter();
    
    @Before
    public final void setupStuff() {
        setReaderAndWriter();
        
        Assert.assertNotNull(reader);
        Assert.assertNotNull(writer);
        
        writer.add("0", "0", "aaabbbccc");
        writer.add("1", "0", "bbbcccddd");
        writer.add("2", "0", "cccdddeee");
        writer.add("3", "0", "dddeeefff");
        writer.add("4", "0", "eeefffggg");
        writer.add("5", "0", "fffggghhh");
    }
    
    @Test
    public void testSimple() throws Exception {
        
        Assert.assertTrue(reader.globSearch("0", "*bbb*").size() > 0);
        
        Assert.assertEquals(Sets.newHashSet("0", "1"), Sets.newHashSet(reader.globSearch("0", "*b*c*"))); // .*b.*c.*
        Assert.assertEquals(Sets.newHashSet("2", "1"), Sets.newHashSet(reader.globSearch("0", "*cd*"))); // .*cd.*
        Assert.assertEquals(Sets.newHashSet("0", "1"), Sets.newHashSet(reader.globSearch("0", "*bbbc*"))); // .*bbbc.*
        Assert.assertEquals(Sets.newHashSet("3"), Sets.newHashSet(reader.globSearch("0", "*ddde*eef*"))); // .*ddde.*eef.*
        
        // an out-of-order that should not work
        Assert.assertEquals(0, reader.globSearch("0", "*c*b*").size());
    }
    
    @Test
    public void testLargeSubstring() {
        Assert.assertTrue(reader.globSearch("0", "*ccdddee*").size() > 0);
    }
    
    // for testing other things...
    
    static final long NUM_BITS;
    static final int SEGMENT_BITS;
    static final int MAX_FILES;
    static final boolean BUILD;
    static final boolean QUERY;
    
    static {
        NUM_BITS = System.getProperty("NUM_BITS") == null ? 16777216 : Long.parseLong(System.getProperty("NUM_BITS"));
        SEGMENT_BITS = System.getProperty("SEGMENT_BITS") == null ? 8192 : Integer.parseInt(System.getProperty("SEGMENT_BITS"));
        MAX_FILES = System.getProperty("MAX_FILES") == null ? 1000 : Integer.parseInt(System.getProperty("MAX_FILES"));
        BUILD = System.getProperty("BUILD") == null ? true : Boolean.parseBoolean(System.getProperty("BUILD"));
        QUERY = System.getProperty("QUERY") == null ? true : Boolean.parseBoolean(System.getProperty("QUERY"));
        
        System.out.println("NUM_BITS " + NUM_BITS);
        System.out.println("SEGMENT_BITS " + SEGMENT_BITS);
        System.out.println("MAX_FILES " + MAX_FILES);
        System.out.println("QUERY " + QUERY);
    }
    
    protected final void queryIndex(IO io) throws Exception {
        if (!QUERY) return;
        
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
        
        
        final Map<Long, String> bitToKey = new HashMap<Long, String>();
        final Map<String, String> lines = new HashMap<String, String>();
        for (File file : listing) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            int lineNumber = 0;
            String line = reader.readLine();
            while (line != null) {
                BitHashableKey<String> key = Hashers.makeHasher(String.class, NUM_BITS).hashableKey(file.getName() + "_" + lineNumber);
                lineNumber += 1;
                bitToKey.put(key.getHashBit(), key.getKey());
                lines.put(key.getKey(), line);
                line = reader.readLine();
            }
        }
        System.out.println("Built reverse bit index");
        
        BareIOTrigramReader<String, String> index = new BareIOTrigramReader<String, String>(NUM_BITS, SEGMENT_BITS)
                .withIO(io)
                .withLookup(new Lookup<String, String, String>() {
                    public String lookup(String key, String field) {
                        return lines.get(key);
                    }

                    public Collection<String> toKeys(long[] bits) {
                        List<String> keys = new ArrayList<String>(bits.length);
                        for (long bit : bits)
                            keys.add(toKey(bit));
                        return keys;
                    }

                    public String toKey(long bit) {
                        return bitToKey.get(bit);
                    }
                });
        
        String[] searchTerms = new String[] {
//                "*ide*",
                "*olt*",
                "*ishonourabl*",
                "*nbu*nin*" // unbuttoning
//                "*ale*",
//                "*anthropophaginian*",
//                "*ber*ask*", // looking for BERGOMASK
//                "*ent*tai*ent*", // looking for ENTERTAINMENT
//                "*entertainment*"
        };
        
        for (String query : searchTerms) {
            long start = System.currentTimeMillis();
            Collection<String> resultKeys = index.globSearch("TEXT", query);
            long end  = System.currentTimeMillis();
            System.out.println(String.format("Query for \"%s\" took %d ms and had %d hits", query, (end-start), resultKeys.size()));
            int count = 0;
            for (String key : resultKeys) {
                System.out.println(String.format("  %s -> %s", key, lines.get(key)));
//                if (++count >= 10) break;
            }
            System.out.println("");
        }
        
    }
    
    // CASSANDRA_HOME=/Users/gdusbabek/Downloads/apache-cassandra-1.2.9 /Users/gdusbabek/Downloads/apache-cassandra-cli < /Users/gdusbabek/codes/github/headwater/headwater-integration-tests/src/integration/resources/load.script
    protected final void buildIndex(IO io) throws Exception {
        if (!BUILD) return;
        
        MemLookupObserver<String, String, String> keyObserver = new MemLookupObserver<String, String, String>();
        BareIOTrigramWriter<String, String> index = new BareIOTrigramWriter<String, String>(NUM_BITS, SEGMENT_BITS, 536870912)
                .withIO(io)
                .withObserver(keyObserver);

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
        
        new Thread("Dumper") { public void run() {
            while (true) {
                try { sleep(10000); } catch (Exception e) {};
                dumpMetrics();
            }
        }}.start();
        
        int fileCount = 0;
        for (File file : listing) {
            if (fileCount >= MAX_FILES)
                break;
            fileCount += 1;
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
//                    System.out.println(lineNumber);
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
                return true;
            }
        });
        
        for (SortedMap<MetricName, Metric> metrics : metricMap.values()) {
            for (Map.Entry<MetricName, Metric> entry : metrics.entrySet()) {
                System.out.print(String.format("%s.%s ", entry.getKey().getClass().getSimpleName(), entry.getKey().getName()));
                if (entry.getValue() instanceof Timer) {
                    Timer timer = (Timer)entry.getValue();
                    Snapshot snapshot = timer.getSnapshot();
                    System.out.println(String.format("count: %d, 98th: %s ", timer.count(), snapshot.get98thPercentile()));
                    
                } else if (entry.getValue() instanceof Counter) {
                    Counter counter = (Counter)entry.getValue();
                    System.out.println(String.format("count: %d", counter.count()));
                } else if (entry.getValue() instanceof Meter) {
                    Meter meter = (Meter)entry.getValue();
                    System.out.println(String.format("count: %d", meter.count()));
                }
            }
        }
        System.out.println();
    }
}