package headwater.examples;

import headwater.Utils;
import headwater.bitmap.BitmapFactory;
import headwater.bitmap.IBitmap;
import headwater.bitmap.MemoryBitmap2;
import headwater.hashing.BitHashableKey;
import headwater.hashing.FunnelHasher;
import headwater.hashing.Hashers;
import headwater.index.DataLookup;
import headwater.index.KeyLookup;
import headwater.index.KeyObserver;
import headwater.index.StandardIndexReader;
import headwater.index.StandardIndexWriter;
import headwater.io.CassandraBitmapIO;
import headwater.io.MemoryBitmapIO;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.ConsoleReporter;

public class Shakespeare {
    
    public static final int SEGMENT_SIZE = 8192;
    public static final long BITMAP_SIZE = 4294967296L;
    
    private static File[] getListing() {
        File dir = new File("src/examples/resources/shakespeare");
        File[] listing = dir.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return !pathname.isHidden();
            }
        });
        return listing;
        
    }
    private static List<ToIndex> readShakespeareFiles() throws IOException {
        List<ToIndex> lines = new ArrayList<ToIndex>();
        for (File file : getListing()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line = reader.readLine();
            int lineNumber = 0;
            while (line != null) {
                line = line.trim();
                if (line.length() > 0)
                    lines.add(new ToIndex(file.getName() + "_" + lineNumber, "TEXT", line));
                lineNumber += 1;
                line = reader.readLine();
            }
        }
        return lines;
    }

    public static void main(String args[]) {
        long s = System.currentTimeMillis();

        buildIndex(args);
        queryIndex(args);

        ConsoleReporter.forRegistry(Utils.getMetricRegistry()).build().report();

        System.out.printf("%nComplete in %.2f seconds%n", ((System.currentTimeMillis() - s) / 1000d));
    }

    public static void queryIndex(String args[]) {
        try {
            CassandraBitmapIO cassandra = new CassandraBitmapIO("127.0.0.1", 9160, "shakespeare", "shakespeare_bitmaps");
            final Map<Long, String> bitToKey = new HashMap<Long, String>();
            final List<ToIndex> lines = readShakespeareFiles();
            final Map<String, String> linesMap = new HashMap<String, String>();
            for (ToIndex line : lines)
                    linesMap.put(line.key, line.value);
    
            // KEY: $fileName_$lineNumber, FIELD: TEXT, VALUE: $line
            DataLookup<String, String, String> dataLookup = new DataLookup<String, String, String>() {
                public String lookup(String key, String field) {
                    return linesMap.get(key);
                }
            };
            KeyLookup<String> keyLookup = new KeyLookup<String>() {
                public Collection<String> toKeys(long[] bits) {
                    List<String> keys = new ArrayList<String>();
                    for (long bit : bits)
                        keys.add(toKey(bit));
                    return keys;
                }
    
                public String toKey(long bit) {
                    return bitToKey.get(bit);
                }
            };
            
            // build bitToKey
            for (ToIndex line : lines) {
                BitHashableKey<String> keyHash = ((FunnelHasher<String>) Hashers.makeHasher(String.class, BITMAP_SIZE)).hashableKey(line.key);
                bitToKey.put(keyHash.getHashBit(), keyHash.getKey());
            }
            
            StandardIndexReader<String, String> reader = new StandardIndexReader<String, String>(SEGMENT_SIZE)
                    .withIO(cassandra)
                    .withDataLookup(dataLookup)
                    .withKeyLookup(keyLookup);
            
            
            String[] queries = new String[] {
                    "*diffidence*", // kingjohn
                    "*lieutenant*", // othello
                    "*dishonest*", // twelthnight
                    "*dif*",
                    "*lie*",
                    "*dis*"
            };
            long start = 0, end = 0;
            for (String query : queries) {
                start = System.currentTimeMillis();
                Collection<String> keys = reader.globSearch("TEXT", query);
                end = System.currentTimeMillis();
                System.out.println(String.format("%s took %d with %d results", query, end-start, keys.size()));
                for (String key : keys) {
                    String line = linesMap.get(key);
                    String[] parts = key.split("_", -1);
                    //System.out.println(String.format(" %s@%s: %s", parts[0], parts[1], line));
                }
            }
        } catch (Throwable ex) {
            ex.printStackTrace(System.err);
            System.exit(0);
        }
    }
    
    public static void buildIndex(String args[]) {
        
        try {
            final Map<Long, String> bitToKey = new HashMap<Long, String>();
            
            // we're going to need a cassandra IO to flush to.
            CassandraBitmapIO cassandra = new CassandraBitmapIO("127.0.0.1", 9160, "shakespeare", "shakespeare_bitmaps");
            MemoryBitmapIO memory = new MemoryBitmapIO().withBitmapFactory(new BitmapFactory() {
                public IBitmap make() {
                    return new MemoryBitmap2(SEGMENT_SIZE);
                }
            });
    
            // read the lines into memory before indexing them.
            System.out.println("Reading in files");
            List<ToIndex> lines = readShakespeareFiles();
            
            KeyObserver<String, String, String> keyObserver = new KeyObserver<String, String, String>() {
                public void observe(BitHashableKey<String> key, String field, String value) {
                    // keep track of bit to key here.
                    bitToKey.put(key.getHashBit(), key.getKey());
                }
            };
            
            StandardIndexWriter<String, String> writer = new StandardIndexWriter<String, String>(SEGMENT_SIZE, BITMAP_SIZE)
                .withObserver(keyObserver)
                .withIO(memory);
            
            System.out.println(String.format("Indexing %d...", lines.size()));
            int lineCount = 0;
            long duration = 0, fstart;
            for (ToIndex line : lines) {
                writer.add(line.key, line.field, line.value);
                lineCount += 1;
                if (lineCount % 1000 == 0) {
                    System.out.print(String.format("Flushing %d...", lineCount));
                    fstart = System.currentTimeMillis();
                    memory.flush(cassandra);
                    duration = System.currentTimeMillis() - fstart;
                    System.out.println(String.format("done %d", duration));
                }
            }
            System.out.println(String.format("Final flush %d", lineCount));
            memory.flush(cassandra);
            
            System.out.println("Saving bit to key...");
            
            
        } catch (Throwable ouch) {
            ouch.printStackTrace(System.err);
            System.exit(0);
        }
        
    }

    private static class ToIndex {
        private final String key;
        private final String field;
        private final String value;

        public ToIndex(String key, String field, String value) {
            this.key = key;
            this.field = field;
            this.value = value;
        }
    }
}
