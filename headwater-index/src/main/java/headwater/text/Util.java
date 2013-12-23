package headwater.text;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import headwater.hash.Hashers;

import java.util.HashSet;
import java.util.Set;

public class Util {
    public static void flipOn(byte[] buf, int bit) {
        int index = bit / 8;
        int bitInByte = bit % 8;
        buf[index] |= 0x01 << bitInByte;
    }
    
    public static void flipOff(byte[] buf, int bit) {
        int index = bit / 8;
        int bitInByte = bit % 8;
        buf[index] &= ~(0x01 << bitInByte);
    }
    
    
    public static Set<Long> hashSetFrom(long[] longs) {
        Set<Long> set = new HashSet<Long>();
        for (long l : longs)
            set.add(l);
        return set;
    }
    
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128(543231);
    
    public static <F> byte[] computeIndexRowKey(F field, Trigram trigram) {
        Hasher hasher = HASH_FUNCTION.newHasher();
        Funnel<F> fieldFunnel = (Funnel<F>) Hashers.funnelFor(field.getClass());
        Funnel<Trigram> trigramFunnel = Hashers.funnelFor(Trigram.class);
        
        hasher.putObject(field, fieldFunnel);
        hasher.putObject(trigram, trigramFunnel);
        
        HashCode indexKeyHash = hasher.hash();
        byte[] indexKey = indexKeyHash.asBytes();
        return indexKey;
    }
}
