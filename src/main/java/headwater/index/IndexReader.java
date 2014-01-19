package headwater.index;

import java.util.Collection;

public interface IndexReader<K, F, V> {
    public Collection<K> globSearch(F field, String valueQuery);
}
