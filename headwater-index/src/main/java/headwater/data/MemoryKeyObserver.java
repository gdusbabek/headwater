package headwater.data;

import headwater.hash.BitHashableKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * You wouldn't want to use this IRL. You'd run into hash collisions as things fill up.
 */
public class MemoryKeyObserver<K, F, V> implements KeyObserver<K, F, V> , Lookup<K, F, V>{
    
    private final Map<Long, K> bitToKeys = new HashMap<Long, K>();
    private final Map<K, Map<F, V>> table = new HashMap<K, Map<F, V>>();
    
    public void observe(BitHashableKey<K> key, F field, V value) {
        bitToKeys.put(key.getHashBit(), key.getKey());
        getMap(key.getKey()).put(field, value);
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

    public V lookup(K key, F field) {
        return getMap(key).get(field);
    }

    private synchronized final Map<F, V> getMap(K key) {
        Map<F, V> map = table.get(key);
        if (map == null)  {
            map = new HashMap<F, V>();
            table.put(key, map);
        }
        return map;
    }
}
