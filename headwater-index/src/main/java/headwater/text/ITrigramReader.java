package headwater.text;

import java.util.Collection;

public interface ITrigramReader<K, F> {
    public Collection<K> globSearch(F field, String query);
}
