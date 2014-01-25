package headwater.index;

import java.util.Collection;

public interface KeyLookup<K> {
    public Collection<K> toKeys(long[] bits);
    public K toKey(long bit);
}
