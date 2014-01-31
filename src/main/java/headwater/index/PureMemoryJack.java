package headwater.index;

import headwater.hashing.BitHashableKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// don't use this in production. It's intended to facilitate testing.  In real life this will be represented by IO and
// and a database.
@Deprecated
public class PureMemoryJack<K, F, V> implements KeyObserver<K, F, V>, DataLookup<K, F, V>, KeyLookup<K> {
    
    private final Map<Long, K> bitToKey = new HashMap<Long, K>();
    private final Map<F, Map<K, V>> store = new HashMap<F, Map<K, V>>();
    
    public PureMemoryJack() { }
    
    // KeyObserver
    
    @Override
    public void observe(BitHashableKey<K> key, F field, V value) {
        bitToKey.put(key.getHashBit(), key.getKey());
        storeMapFor(field).put(key.getKey(), value);
    }
    
    private Map<K, V> storeMapFor(F field) {
        Map<K, V> map = store.get(field);
        
        // double checking means we'll have to synchronize less.
        if (map == null) {
            synchronized (store) {
                map = store.get(field);
                if (map == null) {
                    map = new HashMap<K, V>();
                    store.put(field, map);
                }
            }
        }
        return map;
    }
    
    // Lookup.
    
    @Override
    public V lookup(K key, F field) {
        return storeMapFor(field).get(key);
    }

    @Override
    public Collection<K> toKeys(long[] bits) {
        List<K> keys = new ArrayList<K>();
        for (long bit : bits) {
            keys.add(bitToKey.get(bit));
        }
        return keys;
    }

    @Override
    public K toKey(long bit) {
        return bitToKey.get(bit);
    }
    
    // todo: write the flush parts.
}
