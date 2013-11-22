package headwater.cassandra;

import com.netflix.astyanax.Serializer;
import headwater.data.DataAccess;
import headwater.hash.BitHashableKey;
import headwater.hash.FunnelHasher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class CassandraDataAccess<K, F, V> implements DataAccess<K, F, V> {
    
    private final IO io;
    private final Map<Long, K> bitToKeys;
    private final Serializer<K> keySerializer;
    private final Serializer<F> fieldSerializer;
    private final Serializer<V> valueSerializer;
    
    public CassandraDataAccess(IO io, Class<K> keyClass, Serializer<K> keySerializer, Serializer<F> fieldSerializer, Serializer<V> valueSerializer) {
        this.io = io;
        
        // todo: cardinality warning.
        bitToKeys = new CassMap<Long, K>("bit_to_keys_index".getBytes(), Long.class, keyClass, io);
        
        this.keySerializer = keySerializer;
        this.fieldSerializer = fieldSerializer;
        this.valueSerializer = valueSerializer;
    }
    
    public void observe(BitHashableKey<K> key, F field, V value) {
        bitToKeys.put(key.getHashBit(), key.getKey());
    }

    public V lookup(K key, F field) {
        try {
            return valueBytesToObject(io.get(keyToBytes(key), fieldToBytes(field)));
        } catch (Exception ex) {
            throw new InternalError("Unexpected data read error");
        }
    }

    public Collection<K> toKeys(long[] bits) {
        List<K> results = new ArrayList<K>(bits.length);
        for (long bit : bits)
            results.add(bitToKeys.get(bit));
        return results;
    }

    public K toKey(long bit) {
        return bitToKeys.get(bit);
    }
    
    //
    // helpers
    //
    
    private V valueBytesToObject(byte[] data) {
        return valueSerializer.fromBytes(data);
    }
    
    private byte[] keyToBytes(K key) {
        return keySerializer.toBytes(key);
    }
    
    private byte[] fieldToBytes(F field) {
        return fieldSerializer.toBytes(field);
    }
}
