package headwater.index;

import headwater.hashing.BitHashableKey;

public interface KeyObserver<K, F, V> {
    public void observe(BitHashableKey<K> key, F field, V value);
}