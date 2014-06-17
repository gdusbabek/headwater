package headwater.hashing;

import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;

import headwater.trigram.Trigram;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class Hashers {
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128(543231);
    
    
    private static Funnel<CharSequence> StringFunnel = Funnels.stringFunnel();
    private static Funnel<Integer> IntFunnel = Funnels.integerFunnel();
    private static Funnel<Float> FloatFunnel = new Funnel<Float>() {
        public void funnel(Float from, PrimitiveSink into) {
            into.putFloat(from);
        }
    };
    private static final Funnel<Long> LongFunnel = Funnels.longFunnel();
    private static final Funnel<Double> DoubleFunnel = new Funnel<Double>() {
        public void funnel(Double from, PrimitiveSink into) {
            into.putDouble(from);
        }
    };
    private static final Funnel<Byte> ByteFunnel = new Funnel<Byte>() {
        public void funnel(Byte from, PrimitiveSink into) {
            into.putByte(from);
        }
    };
    private static final Funnel<byte[]> ByteArrayFunnel = Funnels.byteArrayFunnel();
    private static final Funnel<Trigram> TrigramFunnel = new Funnel<Trigram>() {
        public void funnel(Trigram from, PrimitiveSink into) {
            from.sink(into);
        }
    };
    
    public static Funnel<Object> GenericObjectFunnel = new Funnel<Object>() {
        public void funnel(Object from, PrimitiveSink into) {
            Field[] fields = from.getClass().getDeclaredFields();
            AccessibleObject.setAccessible(fields, true);
            for (final Field field : fields) {
                if (!field.getName().contains("$") && !Modifier.isStatic(field.getModifiers())) {
                    // what about transients and such? Include them for now.
                    try {
                        Object fieldValue = field.get(from);
                        into.putInt(fieldValue.hashCode());
                    } catch (IllegalAccessException ex) {
                        throw new InternalError("Unexpected exception while funnling foreign object");
                    }
                }
            }
        }
    };
    
    public static synchronized <T> FunnelHasher<T> makeHasher(final Class<T> cls, long bits) {
        return new FunnelHasher<T>(HASH_FUNCTION, bits) {
            private final Funnel<T> delegate = funnelFor(cls);
            
            @Override
            public void funnel(T from, PrimitiveSink into) {
                delegate.funnel(from, into);
            }
        };
    }
    
    @SuppressWarnings("unchecked")
    public static <T> Funnel<T> funnelFor(Class<T> type) {
        if (String.class.equals(type))
            return (Funnel<T>)StringFunnel;
        else if (Integer.class.equals(type))
            return (Funnel<T>)IntFunnel;
        else if (Float.class.equals(type))
            return (Funnel<T>)FloatFunnel;
        else if (Long.class.equals(type))
            return (Funnel<T>)LongFunnel;
        else if (Double.class.equals(type))
            return (Funnel<T>)DoubleFunnel;
        else if (Byte.class.equals(type))
            return (Funnel<T>)ByteFunnel;
        else if (byte[].class.equals(type)) // does this work or return Array.class?
            return (Funnel<T>)ByteArrayFunnel;
        else if (Trigram.class.equals(type))
            return (Funnel<T>)TrigramFunnel;
        else if (Object.class.equals(type))
            return (Funnel<T>)GenericObjectFunnel;
        else {
            try {
                return (Funnel<T>)(Class.forName(type.getName() + "Funnel").newInstance());
            } catch (Exception ex) {
                IllegalArgumentException bad = new IllegalArgumentException(ex.getMessage());
                bad.setStackTrace(ex.getStackTrace());
                throw bad;
            }
        }
    }
    
    
    // always 128 bits.
    public static <F> byte[] computeIndexRowKey(F field, Trigram trigram) {
        Hasher hasher = HASH_FUNCTION.newHasher();
        @SuppressWarnings("unchecked") Funnel<F> fieldFunnel = (Funnel<F>) Hashers.funnelFor(field.getClass());
        Funnel<Trigram> trigramFunnel = Hashers.funnelFor(Trigram.class);
        
        hasher.putObject(field, fieldFunnel);
        hasher.putObject(trigram, trigramFunnel);
        
        HashCode indexKeyHash = hasher.hash();
        byte[] indexKey = indexKeyHash.asBytes();
        return indexKey;
    }
}
