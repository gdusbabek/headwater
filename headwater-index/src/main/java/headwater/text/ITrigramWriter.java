package headwater.text;

public interface ITrigramWriter<K, F> {
    public void add(K key, F field, String value);
}
