package headwater.text;


import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractTrigramIndexTest {
    
    private ITrigramIndex<String, String> trigramIndex;
    
    public abstract ITrigramIndex<String, String> makeIndex();
    
    @Before
    public final void setupStuff() {
        trigramIndex = makeIndex();
        
        Assert.assertNotNull(trigramIndex);
        
        trigramIndex.add("0", "0", "aaabbbccc");
        trigramIndex.add("1", "0", "bbbcccddd");
        trigramIndex.add("2", "0", "cccdddeee");
        trigramIndex.add("3", "0", "dddeeefff");
        trigramIndex.add("4", "0", "eeefffggg");
        trigramIndex.add("5", "0", "fffggghhh");
    }
    
    @Test
    public void testSimple() throws Exception {
        
        Assert.assertTrue(trigramIndex.globSearch("0", "*bbb*").size() > 0);
        
        Assert.assertEquals(Sets.newHashSet("0", "1"), Sets.newHashSet(trigramIndex.globSearch("0", "*b*c*"))); // .*b.*c.*
        Assert.assertEquals(Sets.newHashSet("2", "1"), Sets.newHashSet(trigramIndex.globSearch("0", "*cd*"))); // .*cd.*
        Assert.assertEquals(Sets.newHashSet("0", "1"), Sets.newHashSet(trigramIndex.globSearch("0", "*bbbc*"))); // .*bbbc.*
        Assert.assertEquals(Sets.newHashSet("3"), Sets.newHashSet(trigramIndex.globSearch("0", "*ddde*eef*"))); // .*ddde.*eef.*
        
        // an out-of-order that should not work
        Assert.assertEquals(0, trigramIndex.globSearch("0", "*c*b*").size());
    }
    
    @Test
    public void testLargeSubstring() {
        Assert.assertTrue(trigramIndex.globSearch("0", "*ccdddee*").size() > 0);
    }
}