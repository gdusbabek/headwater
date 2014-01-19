package headwater.index;

public interface IndexWriter<K, F, V> {
    public void add(K key, F field, V value);
}
