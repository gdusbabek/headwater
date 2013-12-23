package headwater.data;

import headwater.hash.BitHashableKey;

import java.util.Collection;

public interface KeyObserver<K, F, V> {
    public void observe(BitHashableKey<K> key, F field, V value);
}
