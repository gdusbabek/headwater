package headwater.index;

import com.google.common.collect.Sets;
import com.netflix.astyanax.serializers.StringSerializer;
import headwater.bitmap.BitmapFactory;
import headwater.bitmap.IBitmap;
import headwater.bitmap.MemoryBitmap2;
import headwater.hashing.BitHashableKey;
import headwater.hashing.Hashers;
import headwater.io.IO;
import headwater.io.MemoryBitmapIO;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

public class TestIndexing {
    
    private long bitmapLength = 4294967296L;
    private IO<Long, IBitmap> io;
//    private PureMemoryJack<String, String, String> jack;
    private IOJack<String, String, String> jack;
    private StandardIndexReader<String, String> reader;
    private StandardIndexWriter<String, String> writer;
    
    @Before
    public void setup() {
        final int segmentLength = 8192;
        this.io = new MemoryBitmapIO().withBitmapFactory(new BitmapFactory() {
            @Override
            public IBitmap make() {
                return new MemoryBitmap2(segmentLength);
            }
        });
//        this.jack = new PureMemoryJack<String, String, String>();
        this.jack = new IOJack<String, String, String>(StringSerializer.get());
        this.reader = new StandardIndexReader<String, String>(segmentLength).withIO(io).withDataLookup(jack).withKeyLookup(jack);
        this.writer = new StandardIndexWriter<String, String>(segmentLength, bitmapLength).withIO(io).withObserver(jack);
        
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
    public void testSimple() {
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
    
    //@Test
    public void testFullTextIngestAndQuery() throws IOException {
        // todo: clear the reader/writer/io.
        
        if (System.getProperty("SHAKESPEARE_PATH") == null) return;
        
        ingest(System.getProperty("SHAKESPEARE_PATH"));
        query(System.getProperty("SHAKESPEARE_PATH"));
    }
    
    private void query(String path) throws IOException {
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
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                int lineNumber = 0;
                String line = reader.readLine();
                while (line != null) {
                    BitHashableKey<String> key = Hashers.makeHasher(String.class, bitmapLength).hashableKey(
                            file.getName() + "_" + lineNumber);
                    lineNumber += 1;
                    bitToKey.put(key.getHashBit(), key.getKey());
                    lines.put(key.getKey(), line);
                    line = reader.readLine();
                }
            }
        }
        
        String[] queries = new String[] {
                "*pro*ender*",
                "*pr*cy*",
                "*tious*",
                "*pen*out*"
        };
        for (String query : queries) {
            Collection<String> hits = reader.globSearch("TEXT", query);
            System.out.println(String.format("%d hits for %s", hits.size(), query));
            for (String hit : hits) {
                System.out.println(String.format(" %s", lines.get(hit)));
            }
        }
    }
    
    final int MAX_FILES = System.getProperty("MAX_FILES") == null ? 1000 : Integer.parseInt(System.getProperty("MAX_FILES"));
    
    private void ingest(String path) throws IOException {
        File shakespeareDir = new File(path);
        File[] listing = shakespeareDir.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                if (pathname.isHidden())
                    return false;
                if (pathname.getName().equals("glossary"))
                    return false;
                return true;
            }
        });
        
        // read the lines into memory before indexing them.
        List<ToIndex> lines = new ArrayList<ToIndex>();
        int fileCount = 0;
        for (File file : listing) {
            if (fileCount >= MAX_FILES)
                break;
            fileCount += 1;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                String line = reader.readLine();
                int lineNumber = 0;
                while (line != null) {
                    line = line.trim();
                    if (line.length() > 0) lines.add(new ToIndex(file.getName() + "_" + lineNumber, "TEXT", line));
                    lineNumber += 1;
                    line = reader.readLine();
                }
            }
        }
        
        System.out.println(String.format("Will index %d lines", lines.size()));
        int lineCount = 0;
        for (ToIndex line : lines) {
            writer.add(line.key, line.field, line.value);
            lineCount += 1;
            if (lineCount % 1000 == 0)
                System.out.println(String.format("On line %d", lineCount));
        }
    }
    
    private class ToIndex {
        private final String key;
        private final String field;
        private final String value;
        public ToIndex(String key, String field, String value) {
            this.key = key;
            this.field = field;
            this.value = value;
        }
    }
    
    public static void main(String args[]) {
        System.setProperty("MAX_FILES", "2");
        System.setProperty("SHAKESPEARE_PATH", "/Users/gdusbabek/Downloads/shakespeare");
        
        try {
            TestIndexing test = new TestIndexing();
            test.setup();
            test.testFullTextIngestAndQuery();
        } catch (Throwable th) {
            th.printStackTrace(System.err);
        }
    }
}
