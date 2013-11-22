package headwater.hash;

import java.util.Collection;

public interface HashObserver<K> {
    public void observe(K key, long hash);
    public Collection<K> toKeys(long[] bits);
    public K toKey(long bit);
}
