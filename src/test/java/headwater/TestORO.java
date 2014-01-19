package headwater;

import junit.framework.Assert;
import org.apache.oro.text.GlobCompiler;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Matcher;
import org.junit.Before;
import org.junit.Test;

public class TestORO {
    
    private PatternMatcher matcher;
    private Pattern pattern;
    
    @Before
    public void setup() throws Exception {
        GlobCompiler gc = new GlobCompiler();
        pattern = gc.compile("ga*dus*ek");
        matcher = new Perl5Matcher();
    }
    
    @Test
    public void testMatches() {
        Assert.assertTrue(matcher.matches("gadkdkdkkdkdkkdkdkdkdkdkdkdkduekdkdkdkdkdkdusdkakkdskskdksdksdkdsksdksdkdskdskdsksdek", pattern));
        Assert.assertTrue(matcher.matches("garydusbabek", pattern));
    }
    
    @Test
    public void testNonMatches() {
        Assert.assertFalse(matcher.matches("gdusbabek", pattern));
        Assert.assertFalse(matcher.matches("gadkskdkskdkskkdkskdkduskdksksksddkskskdkdk", pattern));
        Assert.assertFalse(matcher.matches("gadkskdkskdkskkdkdkkdkdkdududududduesek", pattern));
    }
}
