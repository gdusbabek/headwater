package headwater.data;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import headwater.text.Trigram;

import java.nio.ByteBuffer;

public class CassSerializers {
    
    public static final Serializer<Integer> INTEGER_SERIALIZER = IntegerSerializer.get();
    public static final Serializer<Long> LONG_SERIALIZER = LongSerializer.get();
    public static final Serializer<Double> DOUBLE_SERIALIZER = DoubleSerializer.get();
    public static final Serializer<Float> FLOAT_SERIALIZER = FloatSerializer.get();
    public static final Serializer<byte[]> BYTES_SERIALIZER = BytesArraySerializer.get();
    public static final Serializer<String> STRING_SERIALIZER = StringSerializer.get();
    public static final Serializer<Trigram> TRIGRAM_SERIALIZER = TrigramSerializer.get();
    
    public static <T> Serializer<T> serializerFor(Class<T> type) {
        if (type.equals(Long.class))
            return (Serializer<T>) LONG_SERIALIZER;
        else if (type.equals(Integer.class))
            return (Serializer<T>) INTEGER_SERIALIZER;
        else if (type.equals(Double.class))
            return (Serializer<T>) DOUBLE_SERIALIZER;
        else if (type.equals(Float.class))
            return (Serializer<T>) FLOAT_SERIALIZER;
        else if (type.equals(byte[].class))
            return (Serializer<T>) BYTES_SERIALIZER;
        else if (type.equals(String.class))
            return (Serializer<T>) STRING_SERIALIZER;
        else if (type.equals(Byte.class))
            return (Serializer<T>) INTEGER_SERIALIZER;
        else if (type.equals(Trigram.class))
            return (Serializer<T>) TRIGRAM_SERIALIZER;
        else
            throw new RuntimeException("Unexpected type: " + type.getClass().getName());
    }
    
    private static class TrigramSerializer extends AbstractSerializer<Trigram> {
        
        private static final TrigramSerializer instance = new TrigramSerializer();
        
        private TrigramSerializer() {}
        
        public static final TrigramSerializer get() { return instance; }
        
        @Override
        public ByteBuffer toByteBuffer(Trigram obj) {
            return Trigram.toBuffer(obj);
        }

        @Override
        public Trigram fromByteBuffer(ByteBuffer byteBuffer) {
            return Trigram.fromBuffer(byteBuffer);
        }
        
        
    }
}
