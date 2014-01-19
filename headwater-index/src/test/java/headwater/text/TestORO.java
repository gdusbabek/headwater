package headwater.text;

import org.apache.oro.text.GlobCompiler;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Matcher;
import org.junit.Test;

public class TestORO {
    
    @Test
    public void testORO() {
        try {
            GlobCompiler gc = new GlobCompiler();
            Pattern p = gc.compile("ga*dus*ek");
            
            PatternMatcher m = new Perl5Matcher();
            
            String[] strings = new String[]{};
            for (String s : strings) {
                System.out.println(String.format("%s: matches? %s", s, m.matches(s, p)));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
