package headwater.index;

import java.util.Collection;

public interface Index<K, F, V> {
    public void add(K key, F field, V value);
    public Collection<K> search(F field, V value);
    public Collection<K> search(F field, Filter<V> filter);
}
