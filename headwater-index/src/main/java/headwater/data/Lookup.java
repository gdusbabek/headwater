package headwater.data;

public interface Lookup<K, F, V> {
    public V lookup(K key, F field);
}
