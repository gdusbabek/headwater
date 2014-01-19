package headwater.hashing;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.PrimitiveSink;

import java.math.BigInteger;
import java.util.Comparator;

/**
 * capable of generating hashcodes concurrently in a threadsafe manner. Have at it.
 * @param <T>
 */
public abstract class FunnelHasher<T> implements Comparator<T> {
    private final HashFunction hashFunction;
    private final Funnel<T> funnel;
    private final BigInteger bits;
    
    public FunnelHasher(HashFunction hashFunction, long bits) {
        this.hashFunction = hashFunction;
        this.bits = new BigInteger(Long.toString(bits));
        funnel = new Funnel<T>() {
            public void funnel(T from, PrimitiveSink into) {
                FunnelHasher.this.funnel(from, into);
            }
        };
    }
    
    public BitHashableKey<T> hashableKey(final T t) {
        return new BitHashableKey<T>() {
            private volatile Long hash = null;
            private volatile byte[] bytes = null;
            
            private void compute() {
                if (hash == null) {
                    HashCode hc = hash(t);
                    bytes = hc.asBytes();
                    BigInteger bi = new BigInteger(this.bytes);
                    hash = bi.mod(bits).longValue();
                }
            }
            
            public long getHashBit() {
                compute();
                return this.hash;
            }

            public T getKey() {
                return t;
            }

            public byte[] asBytes() {
                compute();
                return this.bytes;
            }
        };
    }
    
    public HashCode hash(T t) {
        Hasher hasher = hashFunction.newHasher();
        hasher.putObject(t, funnel);
        return hasher.hash();
    }

    public int compare(T o1, T o2) {
        byte[] b1 = hash(o1).asBytes();
        byte[] b2 = hash(o2).asBytes();
        return bytesCompare(b1, b2);
    }

    @Override
    public boolean equals(Object obj) {
        return obj.equals(this); // dunno how this works.
    }

    public abstract void funnel(T from, PrimitiveSink into);
    
    public static int bytesCompare(byte[] buf, byte[] obuf) {
        // treat each buffer as 8bit unsigned numbers.
        // todo: get smart and do not promote to integers. (this is probably impossbile with bitwise operations in java.)
        for (int i = 0; i < Math.min(buf.length, obuf.length) ; i++) {
            if (buf[i] == obuf[i]) continue;
            else return (buf[i] & 0xff) < (obuf[i] & 0xff) ? -1 : 1;
        }
        
        // shorter identical buffer comes before.
        if (buf.length == obuf.length) return 0;
        else return buf.length - obuf.length;
    }
}
