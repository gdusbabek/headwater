package headwater.trigram;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import headwater.hashing.Hashers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestTrigrams {
    
    @Test
    public void testByteufferAssumptions() {
        byte[] buf = new byte[]{0,11,22,33,44,55,66,77,88,99};
        
        final ByteBuffer bb = ByteBuffer.wrap(buf, 2, 5);
        
        Runnable dumper = new Runnable() { public void run() {
//            System.out.println("pos: " + bb.position());
//            System.out.println("ao : " + bb.arrayOffset());
//            System.out.println("lim: " + bb.limit());
//            System.out.println("cap: " + bb.capacity());
//            System.out.println("");
        }};
        
        dumper.run();
        
        bb.get();
        bb.get();
        
        dumper.run();
        
    }
    
    @Test
    public void testToString() {
        Trigram trigram = Trigram.make("abc").iterator().next();
        Assert.assertEquals("abc", trigram.toString());
    }
    
    @Test
    public void testHashCodeEquality() {
        final int bits = 0x00100000;
        Trigram a = Trigram.make("abc").iterator().next();
        Trigram b = Trigram.make("abc").iterator().next();
        Trigram c = Trigram.make("def").iterator().next();
        HashCode hcA = Hashers.makeHasher(Trigram.class, bits).hash(a);
        HashCode hcAA = Hashers.makeHasher(Trigram.class, bits).hash(a);
        HashCode hcB = Hashers.makeHasher(Trigram.class, bits).hash(b);
        HashCode hcC = Hashers.makeHasher(Trigram.class, bits).hash(c);
        
        Assert.assertTrue(hcA.equals(hcAA));
        Assert.assertTrue(hcA.equals(hcB));
        Assert.assertFalse(hcA.equals(hcC));
        
        Assert.assertEquals(hcA.asInt(), hcAA.asInt());
        Assert.assertEquals(hcA.asInt(), hcB.asInt());
    }
    
    @Test
    public void testEquality() {
        Assert.assertTrue(
                Trigram.make("abc").iterator().next().equals(Trigram.make("abc").iterator().next())
        );
    }
    
    @Test
    public void testTrigramCountAssumptions() {
        List<Trigram> trigrams = (List<Trigram>)Trigram.make("abc");
        Assert.assertEquals(1, trigrams.size());
        
        trigrams = (List<Trigram>)Trigram.make("abcd");
        Assert.assertEquals(2, trigrams.size());
        
        trigrams = (List<Trigram>)Trigram.make("abcde");
        Assert.assertEquals(3, trigrams.size());
        
        trigrams = (List<Trigram>)Trigram.make("abcdef");
        Assert.assertEquals(4, trigrams.size());
        
        trigrams = (List<Trigram>)Trigram.make("abcdefg");
        Assert.assertEquals(5, trigrams.size());
        
        trigrams = (List<Trigram>)Trigram.make("abcdefgh");
        Assert.assertEquals(6, trigrams.size());
        
        trigrams = (List<Trigram>)Trigram.make("abcdefghi");
        Assert.assertEquals(7, trigrams.size());
        
        // each character adds one to the number of possible trigrams
    }

    @Test
    public void testSimpleAscii() {
        List<Trigram> trigrams = (List<Trigram>)Trigram.make("foobar");
        Assert.assertEquals(4, trigrams.size());
        
        byte[] foo = (byte[]) Whitebox.getInternalState(trigrams.get(0), "buf");
        Assert.assertEquals(0, foo[0]);
        Assert.assertEquals(0, foo[1]);
        Assert.assertEquals(0, foo[2]);
        Assert.assertEquals((int)'f', foo[3]);
        Assert.assertEquals(0, foo[4]);
        Assert.assertEquals(0, foo[5]);
        Assert.assertEquals(0, foo[6]);
        Assert.assertEquals((int)'o', foo[7]);
        Assert.assertEquals(0, foo[8]);
        Assert.assertEquals(0, foo[9]);
        Assert.assertEquals(0, foo[10]);
        Assert.assertEquals((int)'o', foo[11]);
        
        byte[] bar = (byte[])Whitebox.getInternalState(trigrams.get(3), "buf");
        Assert.assertEquals(0, bar[0]);
        Assert.assertEquals(0, bar[1]);
        Assert.assertEquals(0, bar[2]);
        Assert.assertEquals((int)'b', bar[3]);
        Assert.assertEquals(0, bar[4]);
        Assert.assertEquals(0, bar[5]);
        Assert.assertEquals(0, bar[6]);
        Assert.assertEquals((int)'a', bar[7]);
        Assert.assertEquals(0, bar[8]);
        Assert.assertEquals(0, bar[9]);
        Assert.assertEquals(0, bar[10]);
        Assert.assertEquals((int)'r', bar[11]);    
    }
    
    @Test
    public void testUnicode2Byte() {
        // here is a two byte code point (3 byte encoded) example.
        Trigram trigram = Trigram.make("\u20ac\u20ac\u20ac").iterator().next(); // € = U+20AC
        byte[] buf = (byte[])Whitebox.getInternalState(trigram, "buf");
        
        // first two bytes should be zero, next two should have stuff.
        Assert.assertEquals(0, buf[0]);
        Assert.assertEquals(0, buf[1]);
        Assert.assertNotSame(0, buf[2]);
        Assert.assertNotSame(0, buf[2]);
    }
    
    @Test
    public void testUnicode3Byte() {
        // here is a three byte code point (4 byte encoded) example.
        Trigram trigram = Trigram.make(new String(new byte[]{-16, -99, -124, -94, -16, -99, -124, -94, -16, -99, -124, -94}, Charsets.UTF_8)).iterator().next();// F clef (3 of them)
        byte[] buf = (byte[])Whitebox.getInternalState(trigram, "buf");
        
        // first byte is zero.
        Assert.assertEquals(0, buf[0]);
        Assert.assertNotSame(0, buf[1]);
        Assert.assertNotSame(0, buf[2]);
        Assert.assertNotSame(0, buf[2]);
        
        // I haven't dug enough yet to know how to produce valid 4 byte code points. sigh.
    }
    
    @Test
    public void testComparingEquality() {
        // equality works and is reflexive.
        Trigram aaa = Trigram.make("aaa").iterator().next();
        Trigram alsoaaa = Trigram.make("aaa").iterator().next();
        Assert.assertEquals(0, aaa.compareTo(alsoaaa));
        Assert.assertEquals(0, alsoaaa.compareTo(aaa));
    }

    @Test
    public void testComparingSimple() {
        Trigram aaa = Trigram.make("aaa").iterator().next();
        Trigram aab = Trigram.make("aab").iterator().next();
        Trigram baa = Trigram.make("baa").iterator().next();
        
        Assert.assertTrue(aaa.compareTo(aab) < 0);
        Assert.assertTrue(aab.compareTo(aaa) > 0);
        
        Assert.assertTrue(aab.compareTo(baa) < 0);
        Assert.assertTrue(baa.compareTo(aab) > 0);
    }
    
    @Test
    public void testComparingHigh() {
        // bytes greater than 0x80 get converted to negative numbers during an int cast in java. make sure this doesn't
        // screw with comparisons.
        Trigram hi = Trigram.make(new byte[] {-127, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}).iterator().next();
        Trigram lo = Trigram.make(new byte[] {5 , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}).iterator().next();
        
        // lo should come before high.
        Assert.assertTrue(lo.compareTo(hi) < 0);
        Assert.assertTrue(hi.compareTo(lo) > 0);
    }
    
    @Test
    public void testContains() {
        Trigram foo = Trigram.make("foo").iterator().next();
        Assert.assertTrue(foo.contains("fo"));
        Assert.assertTrue(foo.contains("oo"));
        Assert.assertTrue(foo.contains("o"));
        Assert.assertTrue(foo.contains("f"));
        Assert.assertFalse(foo.contains("z"));
        Assert.assertFalse(foo.contains("fooo"));
        Assert.assertFalse(foo.contains("ooo"));
        Assert.assertFalse(foo.contains("oz"));
        Assert.assertTrue(foo.contains(""));
    }
    
    @Test
    public void testPadding() {
        List<String> pads = new ArrayList<String>();
        for (String pad : AsciiAugmentationStrategy.padding(1)) {
            pads.add(pad);
        }
        
        // a-z == 26.
        Assert.assertEquals(26, pads.size());
        
        pads = new ArrayList<String>();
        for (String pad : AsciiAugmentationStrategy.padding(2)) {
            pads.add(pad);
        }
        
        Assert.assertEquals(26 + 26 * 26, pads.size());
    }
    
    @Test
    public void testSingleAugmentation() {
        Set<Trigram> trigrams = new HashSet<Trigram>();
        int count = 0;
        for (Trigram trigram : Trigram.make("fo", new AsciiAugmentationStrategy())) {
            trigrams.add(trigram);
            count += 1;
        }
        Assert.assertEquals(26 + 26, trigrams.size());
        Assert.assertEquals(trigrams.size(), count);
    }
    
    @Test
    public void testDoubleAugmentation() {
        Set<Trigram> trigrams = new HashSet<Trigram>();
        int count = 0;
        for (Trigram trigram : Trigram.make("z", new AsciiAugmentationStrategy())) {
            trigrams.add(trigram);
            count += 1;
        }
        
        // minus 26 to account for the z*z duplicates.
        Assert.assertEquals(26 * 26 * 2 - 26, trigrams.size());
        Assert.assertEquals(trigrams.size(), count);
    }
    // todo: need tests to make sure binary trigram create does indeed pad.
}

