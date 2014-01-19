package headwater.index;

import java.util.Collection;

public interface Lookup<K, F, V> {
    public V lookup(K key, F field);
    
    public Collection<K> toKeys(long[] bits);
    public K toKey(long bit);
}
