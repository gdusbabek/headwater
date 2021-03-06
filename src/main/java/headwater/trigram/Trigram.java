package headwater.trigram;

import com.google.common.hash.PrimitiveSink;
import headwater.Utils;
import headwater.hashing.FunnelHasher;
import headwater.hashing.Hashers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

// todo: Convert this into a generic Ngram class.
public class Trigram implements Comparable<Trigram> {
    
    public static final int N = 3;
    private static final int WIDTH_IN_BYTES = Integer.SIZE / 8 * N; // 3 ints wide (currently 12 bytes).
    
    // consider keeping them integers. the ony time they really need to be bytes is on the back end (serialization).
    private final byte[] buf = new byte[WIDTH_IN_BYTES];
    private final int[] ints = new int[N];
    
    private Trigram(int[] ints) {
        // convert to bytes.
        assert ints.length == N;
        for (int i = 0; i < ints.length; i++) {
            buf[i * 4] = (byte)((ints[i] >>> 24) & 0x000000ff);
            buf[i * 4 + 1] = (byte)((ints[i] >>> 16) & 0x000000ff);
            buf[i * 4 + 2] = (byte)((ints[i] >>> 8) & 0x000000ff);
            buf[i * 4 + 3] = (byte)((ints[i] >>> 0) & 0x000000ff);
        }
        for (int i = 0; i < N; i++)
            this.ints[i] = ints[i];
    }
    
    // ACHTUNG! raw construction!!!
    private Trigram(byte[] buf, int start, int length) {
        System.arraycopy(buf, start, this.buf, 0, Math.min(WIDTH_IN_BYTES, buf.length - start));
        // assume we get the zeros at the end for free.
        for (int i = 0; i < N; i++)
            this.ints[i] = readInt(buf, i * 4);
    }
    
    public static Trigram fromBuffer(ByteBuffer bb) {
        int limit = bb.limit();
        int pos = bb.position();
        bb.position(limit); // indicates we used those bytes.
        return new Trigram(bb.array(), pos, limit - pos);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Trigram)) return false;
        Trigram other = (Trigram)obj;
        for (int i = 0; i < buf.length; i++)
            if (buf[i] != other.buf[i])
                return false;
        return true;
    }
    
    public boolean contains(String s) {
        List<Integer> codePoints = new ArrayList<Integer>();
        for (int i = 0; i < s.length();) {
            int cp = s.codePointAt(i);
            i += Character.charCount(cp);
            codePoints.add(cp);
        }
        
        for (int i = 0; i < ints.length; i++) {
            if (codePoints.size() > 0 && ints[i] == codePoints.get(0))
                codePoints.remove(0);
        }
        return codePoints.size() == 0;
    }

    private static final FunnelHasher<Trigram> hasher = Hashers.makeHasher(Trigram.class, Integer.MAX_VALUE);
    
    // todo: you only have to do this if you keep them in hash objects. useful for testing?
    @Override
    public int hashCode() {
        return hasher.hash(this).asInt();
    }

    public int compareTo(Trigram o) {
        return FunnelHasher.bytesCompare(buf, o.buf);
    }
    
    public void sink(PrimitiveSink sink) {
        sink.putBytes(buf);
    }
    
    public static ByteBuffer toBuffer(Trigram t) {
        return ByteBuffer.allocate(t.buf.length).put(t.buf);
    }
    
    public String toString() {
        // there are N 4byte chars here.
        // convert each byte to a code point, each codepoint to char.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < N; i++) {
            sb = sb.append(Character.toChars(readInt(buf, i * 4)));
        }
        return sb.toString();
    }
    
    private static int readInt(byte[] buf, int start) {
        int i1 = buf[start] & 0x000000ff;
        int i2 = buf[start + 1] & 0x000000ff;
        int i3 = buf[start + 2] & 0x000000ff;
        int i4 = buf[start + 3] & 0x000000ff;
        return ((i1 << 24) + (i2 << 16) + (i3 << 8) + (i4 << 0));
    }
    
    // todo: dunno about this. I thought each trigram was 12 bytes?
    public static Iterable<Trigram> make(byte[] bytes) {
        List<Trigram> list = new ArrayList<Trigram>();
        for (int i = 0; i < bytes.length; i += N)
            list.add(new Trigram(bytes, i, N));
        // stragglers get handled in the constructor.
        return list;
    }
    
    /** create a bunch of trigrams. pad with zeros (0x00, not 0x30). */
    
    public static Iterable<Trigram> make(String s) {
        return makeOverlapping(s, null);
    }
    
    // one query though, we can get away with testing against trigrams that do not overlap.
    public static Iterable<Trigram>makeNonOverlapping(String s, AugmentationStrategy augmentation) {
        List<Trigram> list = new ArrayList<Trigram>();
        Queue<Integer> buf = new LinkedList<Integer>();
        for (int i = 0; i < s.length();) {
            int cp = s.codePointAt(i);
            buf.add(cp);
            if (buf.size() == N) {
                list.add(new Trigram(Utils.unbox(buf.toArray(new Integer[N]))));
                buf.clear();
            }
            i += Character.charCount(cp);
        }
        
        if (list.size() == 0 && augmentation != null) {
            for (Trigram trigram : augmentation.augment(s))
                list.add(trigram);
        }
        
        return list;
    }
    
    // when we index something, we usually want to index all trigrams (they overlap each other).
    public static Iterable<Trigram> makeOverlapping(String s, AugmentationStrategy augmentation) {
        Set<Integer> listHas = new HashSet<Integer>();
        List<Trigram> list = new ArrayList<Trigram>();
        Queue<Integer> buf = new LinkedList<Integer>();
        boolean waxing = true;
        int[] bufAsBytes;
        int hash;
        for (int i = 0; i < s.length();) {
            int cp = s.codePointAt(i);
            if (!waxing)
                buf.remove();
            buf.add(cp);
            if (buf.size() == N) {
                waxing = false;
                bufAsBytes = Utils.unbox(buf.toArray(new Integer[N]));
                hash = Arrays.hashCode(bufAsBytes);
                if (!listHas.contains(hash)) {
                    list.add(new Trigram(bufAsBytes));
                    listHas.add(hash);
                }
            }
            i += Character.charCount(cp);
        }
        
        if (list.size() == 0 && augmentation != null) {
            for (Trigram trigram : augmentation.augment(s))
                list.add(trigram);
        }
        
        return list;
    }
}
