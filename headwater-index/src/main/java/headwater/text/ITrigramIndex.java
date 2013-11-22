package headwater.text;

import java.util.Collection;

public interface ITrigramIndex<K, F> {
    
    public void add(K key, F field, String value);
    public Collection<K> globSearch(F field, String query);
}
