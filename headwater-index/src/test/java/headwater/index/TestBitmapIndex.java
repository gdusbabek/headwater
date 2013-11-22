package headwater.index;

import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import headwater.bitmap.BitmapFactory;
import headwater.bitmap.IBitmap;
import headwater.bitmap.OpenBitmap;
import headwater.data.DataAccess;
import headwater.hash.Hashers;
import headwater.data.MemoryDataAccess;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TestBitmapIndex {
    
    private DataAccess<String, String, String> observer;
    private BitmapFactory bitmapFactory;
    
    @Before
    public void setUpHelpers() {
        final int numBits = 0x00100000; // 1 million bits.
        observer = new MemoryDataAccess<String, String, String>();
        bitmapFactory = new BitmapFactory() {
            public IBitmap newBitmap(int numBits) {
                throw new RuntimeException("Not implemented");
            }

            public IBitmap newBitmap() {
                //return new JuBitmap(numBits);
                return new OpenBitmap(numBits);
            }
        };
    }
    
    @Test
    public void testGuavaMurmurAssumptions() {
        HashFunction hashFunction = Hashing.murmur3_128();
        Hasher hasher = hashFunction.newHasher();
        Random rand = new Random(12345L);
        for (int i = 0; i < 5000; i++) {
            hasher.putInt(rand.nextInt());
        }
        HashCode code = hasher.hash();
        BigInteger number = new BigInteger(code.asBytes());
        BigInteger bitsInMap = new BigInteger("1048576", 10);
        BigInteger theBit = number.mod(bitsInMap);
        
        System.out.println(String.format("bits in map  : %s", bitsInMap.toString(10)));
        System.out.println(String.format("128 bit hash : %s", number.toString(10)));
        System.out.println(String.format("bit to assert: %s", theBit.toString(10)));
        System.out.println(String.format("bit to assert: %d", theBit.intValue()));
    }
    
    
    @Test
    public void testIndex() throws Throwable {
        final int bits = 0x00100000;
        BitmapIndex<String, String, String> index = new BitmapIndex<String, String, String>(
                Hashers.makeHasher(String.class, bits), 
                Hashers.makeHasher(String.class, bits), 
                Hashers.makeHasher(String.class, bits))
            .withBitmapFactory(bitmapFactory)
            .withObserver(observer);
        
        index.add("row 0", "col 0", "foo");
        index.add("row 1", "col 0", "bar");
        index.add("row 2", "col 0", "foo");
        index.add("row 3", "col 0", "bar");
        index.add("row 4", "col 0", "baz");
        
        index.add("row 0", "col 1", "aaa");
        index.add("row 1", "col 1", "bbb");
        index.add("row 2", "col 1", "ccc");
        index.add("row 3", "col 1", "ddd");
        index.add("row 4", "col 1", "eee");
        
        Set<String> keys = new HashSet<String>(index.search("col 0", "foo"));
        Assert.assertTrue(keys.contains("row 0"));
        Assert.assertFalse(keys.contains("row 1"));
        Assert.assertTrue(keys.contains("row 2"));
        Assert.assertFalse(keys.contains("row 3"));
        Assert.assertFalse(keys.contains("row 4"));
        
        Assert.assertTrue(index.contains("col 0", "foo"));
        Assert.assertFalse(index.contains("col 0", "zip"));
        Assert.assertFalse(index.contains("col 0", "aaa"));
        
    }
    
    @Test
    public void testSimpleSliceFilter() throws Throwable {
        final int bits = 0x00100000;
        BitmapIndex<String, String, String> index = new BitmapIndex<String, String, String>(
                Hashers.makeHasher(String.class, bits), 
                Hashers.makeHasher(String.class, bits), 
                Hashers.makeHasher(String.class, bits))
                .withBitmapFactory(bitmapFactory)
                .withObserver(observer);
        
        index.add("0", "0", "aaa");
        index.add("1", "0", "aaa");
        index.add("2", "0", "zzz"); // will never match.
        index.add("3", "0", "bbb"); 
        index.add("4", "0", "ccc");
        index.add("5", "0", "abc");
        
        // illustrate that a slice need not return a contiguous set of results.
        Filter<String> aStar = new Filter<String>() {
            public boolean matches(String s) {
                return s.matches(".*a.*");
            }
        };
        Filter<String> bStar = new Filter<String>() {
            public boolean matches(String s) {
                return s.matches(".*b.*");
            }
        };
        Filter<String> cStar = new Filter<String>() {
            public boolean matches(String s) {
                return s.matches(".*c.*");
            }
        };
        Filter<String> dStar= new Filter<String>() {
            public boolean matches(String s) {
                return s.matches(".*d.*");
            }
        };
        
        Collection<String> hits = index.search("0", aStar);
        Assert.assertEquals(Sets.newHashSet("0", "1", "5"), new HashSet<String>(hits));
        
        hits = index.search("0", bStar);
        Assert.assertEquals(Sets.newHashSet("3", "5"), new HashSet<String>(hits));
        
        hits = index.search("0", cStar);
        Assert.assertEquals(Sets.newHashSet("4", "5"), new HashSet<String>(hits));
        
        hits = index.search("0", dStar);
        Assert.assertEquals(0, hits.size());                       
    }
    
}
