package headwater.index;

public interface DataLookup<K, F, V> {
    public V lookup(K key, F field);
}