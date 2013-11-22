package headwater.text;


import com.google.common.collect.Sets;
import headwater.bitmap.BitmapFactory;
import headwater.bitmap.IBitmap;
import headwater.bitmap.OpenBitmap;
import headwater.data.DataAccess;
import headwater.data.MemoryDataAccess;
import headwater.hash.FunnelHasher;
import headwater.hash.Hashers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTrigramIndex {
    
    private DataAccess<String, String, String> observer;
    private BitmapFactory bitmapFactory;
    
    @Before
    public void setupStuff() {
        final int bits = 0x00100000;
        observer = new MemoryDataAccess<String, String, String>();
        bitmapFactory = new BitmapFactory() {
            public IBitmap newBitmap(int numBits) {
                throw new RuntimeException("Not implemented");
            }

            public IBitmap newBitmap() {
                return new OpenBitmap(bits);
            }
        };
    }
    
    @Test
    public void testSimple() throws Exception {
        
        FunnelHasher<String> keyHasher = Hashers.makeHasher(String.class, 128);
        TrigramIndex<String, String> index = new TrigramIndex<String, String>(
                keyHasher, 
                Hashers.makeHasher(String.class, 128), 
                128)
                .withBitmapFactory(bitmapFactory)
                .withIndexObserver(observer);
        index.add("0", "0", "aaabbbccc");
        index.add("1", "0", "bbbcccddd");
        index.add("2", "0", "cccdddeee");
        index.add("3", "0", "dddeeefff");
        index.add("4", "0", "eeefffggg");
        index.add("5", "0", "fffggghhh");
        
        Assert.assertEquals(Sets.newHashSet("0", "1"), Sets.newHashSet(index.globSearch("0", "*b*c*"))); // .*b.*c.*
        Assert.assertEquals(Sets.newHashSet("2", "1"), Sets.newHashSet(index.globSearch("0", "*cd*"))); // .*cd.*
        Assert.assertEquals(Sets.newHashSet("0", "1"), Sets.newHashSet(index.globSearch("0", "*bbbc*"))); // .*bbbc.*
        Assert.assertEquals(Sets.newHashSet("3"), Sets.newHashSet(index.globSearch("0", "*ddde*eef*"))); // .*ddde.*eef.*
        
        // an out-of-order that should not work
        Assert.assertEquals(0, index.globSearch("0", "*c*b*").size());
    }
}