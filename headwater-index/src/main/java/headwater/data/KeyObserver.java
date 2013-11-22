package headwater.data;

import headwater.hash.BitHashableKey;

import java.util.Collection;

public interface KeyObserver<K, F, V> {
    public void observe(BitHashableKey<K> key, F field, V value);
    
    public Collection<K> toKeys(long[] bits);
    public K toKey(long bit);
}
