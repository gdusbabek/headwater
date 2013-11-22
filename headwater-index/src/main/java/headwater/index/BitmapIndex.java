package headwater.index;

import com.google.common.collect.TreeBasedTable;
import headwater.bitmap.BitmapFactory;
import headwater.bitmap.IBitmap;
import headwater.bitmap.Utils;
import headwater.data.KeyObserver;
import headwater.hash.BitHashableKey;
import headwater.hash.FunnelHasher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public class BitmapIndex<K, F, V> implements Index<K, F, V> {

    private final FunnelHasher keyHasher;
    private final FunnelHasher fieldHasher;
    private final FunnelHasher valueHasher;
    
    // F -> V -> bitmap
    private final TreeBasedTable<F, V, IBitmap> bitmaps;
    
    private BitmapFactory bitmapFactory;
    private KeyObserver<K, F, V> bitToKey = null;
    
    public BitmapIndex(FunnelHasher<K> keyHasher, FunnelHasher<F> fieldHasher, FunnelHasher<V> valueHasher) {
        this.keyHasher = keyHasher;
        this.fieldHasher = fieldHasher;
        this.valueHasher = valueHasher;
        bitmaps = TreeBasedTable.create(fieldHasher, valueHasher);
    }
    
    public BitmapIndex<K, F, V> withBitmapFactory(BitmapFactory factory) {
        this.bitmapFactory = factory;
        return this;
    }
    
    public BitmapIndex<K, F, V> withObserver(KeyObserver<K, F, V> observer) {
        this.bitToKey = observer;
        return this;
    }
    
    //
    // index interface.
    //
    
    public void add(K key, F field, V value) {
        BitHashableKey<K> bitKey = keyHasher.hashableKey(key);
        getBitmap(field, value).set(bitKey.getHashBit());
        if (bitToKey != null)
            bitToKey.observe(bitKey, field, value);
    }

    public Collection<K> search(F field, V value) {
        long[] assertedBits = filter(field, value);
        Collection<K> keyHits = bitToKey.toKeys(assertedBits);
        return keyHits;
    }

    public Collection<K> search(F field, Filter<V> filter) {
        Set<Long> bitResults = _filter(field, filter);
        Collection<K> keyHits = new ArrayList<K>(bitResults.size());
        for (long bit : bitResults)
            keyHits.add(bitToKey.toKey(bit));
        return keyHits;
    }
    
    // returns the bits in the index that are asserted.
    public long[] filter(F field, V value) {
        return getBitmap(field, value).getAsserted();
    }
    
    public long[] filter(F field, Filter<V> filter) {
        Set<Long> results = _filter(field, filter);
        return Utils.unbox(results.toArray(new Long[results.size()]));
    }
    
    private Set<Long> _filter(F field, Filter<V> filter) {
        // todo: using a hashset here is wrong.
        Set<Long> bitResults = new HashSet<Long>(); 
        SortedMap<V, IBitmap> rowMaps = bitmaps.row(field);
        // since we don't have an index, we just iterate.
        // todo: optimize this.
        for (Map.Entry<V, IBitmap> e : rowMaps.entrySet()) {
            if (filter.matches(e.getKey())) {
                for (long bit: e.getValue().getAsserted()) {
                    bitResults.add(bit);
                }
            }
        }
        return bitResults;
    }
    
    

    public boolean contains(F field, V value) {
        return search(field, value).size() > 0;
    }
    
    //
    // helpers.
    //
    
    // creates new bitmaps on demand.
    private IBitmap getBitmap(F field, V value) {
        IBitmap bm;
        synchronized (bitmaps) {
            bm = bitmaps.get(field, value);
            if (bm == null) {
                bm = bitmapFactory.newBitmap();
                bitmaps.put(field, value, bm);
            }
        }
        return bm;
    }
}
