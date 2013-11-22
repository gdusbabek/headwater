package headwater.data;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.LongSerializer;
import headwater.hash.BitHashableKey;

import java.io.IOError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class IOLookupObserver<K, F, V> implements KeyObserver<K, F, V>, Lookup<K, F, V> {
    private static final byte[] LONG_ROW_KEY = "LONG_ROW_KEY".getBytes();
    
    private final IO lookupIO;
    private final IO bitIO;
    private final boolean longRow;
    private final Serializer<K> keySerializer;
    private final Serializer<F> fieldSerializer;
    private final Serializer<V> valueSerializer;
    
    public IOLookupObserver(IO lookupIO, IO bitIO, boolean longRow, Serializer<K> keySerializer, Serializer<F> fieldSerializer, Serializer<V> valueSerializer) {
        this.lookupIO = lookupIO;
        this.bitIO = bitIO;
        this.longRow = longRow;
        this.keySerializer = keySerializer;
        this.fieldSerializer = fieldSerializer;
        this.valueSerializer = valueSerializer;
    }
    
    //
    // KeyObserver
    //

    public void observe(BitHashableKey<K> key, F field, V value) {
        
        byte[] serializedKey = keySerializer.toBytes(key.getKey());
        
        try {
            lookupIO.put(
                    serializedKey,
                    fieldSerializer.toBytes(field),
                    valueSerializer.toBytes(value)
            );
        } catch (Exception ex) {
            throw new IOError(ex);
        }
        
        try {
            
            byte[] serializedBit = LongSerializer.get().toBytes(key.getHashBit());
            byte[] keyBytes = longRow ? LONG_ROW_KEY : serializedBit; 
            
            bitIO.put(keyBytes, serializedBit, serializedKey);
        } catch (Exception ex) {
            throw new IOError(ex);
        }
    }

    public Collection<K> toKeys(long[] bits) {
        List<K> results = new ArrayList<K>(bits.length);
        for (long bit : bits)
            results.add(toKey(bit));
        return results;
    }

    public K toKey(long bit) {
        byte[] serializedBit = LongSerializer.get().toBytes(bit);

        try {
            byte[] serializedKey = bitIO.get(
                    longRow ? LONG_ROW_KEY : serializedBit,
                    serializedBit
            );
            return keySerializer.fromBytes(serializedKey);
        } catch (Exception ex) {
            throw new IOError(ex);
        }
    }
    
    //
    // Lookup
    //

    public V lookup(K key, F field) {
        try {
            byte[] valueBytes = lookupIO.get(
                    keySerializer.toBytes(key),
                    fieldSerializer.toBytes(field));
            return valueSerializer.fromBytes(valueBytes);
        } catch (Exception ex) {
            throw new IOError(ex);
        }
    }
}
