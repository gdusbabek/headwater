package headwater.fun;

import headwater.data.FakeCassandraIO;
import headwater.data.IO;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class TestIOMap {
    
    private static final String AS = "aaaaaaa";
    private static final String BS = "bbbbbbb";
    private static final String CS = "ccccccc";
    
    private static Format numFormat = new DecimalFormat("000");
    
    private IO io;
    private Map<Long, String> map;
    
    @Before
    public void setupMap() {
        io = new FakeCassandraIO();
        map = new IOMap<Long, String>(new byte[]{0,0,0,0,1}, Long.class, String.class, io);
        
        int numCols = 100;
        for (long L = 0; L < numCols; L++) {
            map.put(L, numFormat.format(L) + "_str");
        }
        
        Assert.assertEquals(numCols, map.size());
    }
    
    @Test
    public void testPutAndGet() {
        // test overwrite.
        map.put(21L, AS);
        Assert.assertEquals(AS, map.get(21L));
        
        map.put(21L, BS);
        Assert.assertEquals(BS, map.get(21L));
        
        // test other columns don't pollute.
        map.put(22L, CS);
        Assert.assertEquals(BS, map.get(21L));
        Assert.assertEquals(CS, map.get(22L));
    }
    
    @Test
    public void testMetaRoKeysDoNotChange() {
        IOMap<String, String> smap0 = new IOMap<String, String>(new byte[]{0}, String.class, String.class, io);
        IOMap<String, String> smap1 = new IOMap<String, String>(new byte[]{0}, String.class, String.class, io);
        
        byte[] metaKey0 = (byte[])Whitebox.getInternalState(smap0, "mapMetaRowKey");
        byte[] metaKey1 = (byte[])Whitebox.getInternalState(smap1, "mapMetaRowKey");
        
        Assert.assertFalse(metaKey0 == metaKey1);
        Assert.assertTrue(metaKey0.length == metaKey1.length);
        for (int i = 0; i < metaKey0.length; i++)
            Assert.assertEquals(metaKey0[i], metaKey1[i]);
    }
    
    @Test
    public void testGetAll() {
        int numEntries = 100;
        
        Set<Long> keys = map.keySet();
        Assert.assertEquals(numEntries, keys.size());
        long curKey = 0;
        for (long key : keys)
            Assert.assertEquals(curKey++, key);
        
        Collection<String> values = new TreeSet<String>(map.values());
        Assert.assertEquals(numEntries, values.size());
        curKey = 0;
        for (String value : values)
            Assert.assertEquals(numFormat.format(curKey++) + "_str", value);
        
        Set<Map.Entry<Long, String>> entries = new TreeSet<Map.Entry<Long, String>>(new Comparator<Map.Entry<Long, String>>() {
            public int compare(Map.Entry<Long, String> o1, Map.Entry<Long, String> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        entries.addAll(map.entrySet());
        Assert.assertEquals(numEntries, entries.size());
        curKey = 0;
        for (Map.Entry<Long, String> entry : entries){
            Assert.assertEquals(curKey, (long)entry.getKey());
            Assert.assertEquals(numFormat.format(curKey) + "_str", entry.getValue());
            curKey += 1;
        }
    }
    
    @Test
    public void testClear() {
        Assert.assertEquals(100, map.size());
        
        map.clear();
        Assert.assertEquals(0, map.size());
    }
    
    @Test
    public void testGetNullWorks() {
        Assert.assertNull(map.get(9999999L));
    }
    
    @Test
    public void testRemove() {
        final Long key = 25L;
        Assert.assertTrue(map.containsKey(key));
        Assert.assertNotNull(map.get(key));
        String gone = map.remove(key);
        Assert.assertEquals(numFormat.format(key) + "_str", gone);
        Assert.assertFalse(map.containsKey(key));
        Assert.assertNull(map.get(key));
    }
    
    @Test
    public void testRemoveNonExisting() {
        Assert.assertNull(map.remove(9999999L));
    }
}
