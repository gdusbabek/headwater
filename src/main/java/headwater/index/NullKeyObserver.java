package headwater.index;

import headwater.hashing.BitHashableKey;

public class NullKeyObserver<K, F, V> implements KeyObserver<K, F, V> {
    public void observe(BitHashableKey<K> key, F field, V value) { }
}
