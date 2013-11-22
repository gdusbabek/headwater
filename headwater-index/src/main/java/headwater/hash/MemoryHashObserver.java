package headwater.hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * You wouldn't want to use this IRL. You'd run into hash collisions as things fill up.
 */
public class MemoryHashObserver<K> implements HashObserver<K> {
    
    private final Map<Long, K> bitToKeys = new HashMap<Long, K>();
    
    public void observe(K key, long hash) {
        bitToKeys.put(hash, key);
    }
    
    public Collection<K> toKeys(long[] bits) {
        List<K> results = new ArrayList<K>(bits.length);
        for (long bit : bits)
            results.add(bitToKeys.get(bit));
        return results;
    }

    public K toKey(long bit) {
        return bitToKeys.get(bit);
    }
}
