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
    
    private Map<Long, K> bitToKey = new HashMap<Long, K>();
    private Map<CompositeKey, V> store = new HashMap<CompositeKey, V>();
    
    public PureMemoryJack() { }
    
    // KeyObserver
    
    @Override
    public void observe(BitHashableKey<K> key, F field, V value) {
        bitToKey.put(key.getHashBit(), key.getKey());
        store.put(new CompositeKey<K, F>(key.getKey(), field), value);
    }
    
    // Lookup.
    
    @Override
    public V lookup(K key, F field) {
        return store.get(new CompositeKey<K, F>(key, field));
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
    
    // help.
    
    private static class CompositeKey<K, F> {
        private final K k;
        private final F f;
        private final Integer hashCode;
        
        public CompositeKey(K k, F f) {
            this.k = k;
            this.f = f;
            this.hashCode = k.hashCode() ^ f.hashCode();
        }
        
        @Override
        public int hashCode() {
            return hashCode;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof CompositeKey))
                return false;
            CompositeKey other = (CompositeKey)obj;
            return other.k.equals(this.k) && other.f.equals(this.f);
        }
    }
}
