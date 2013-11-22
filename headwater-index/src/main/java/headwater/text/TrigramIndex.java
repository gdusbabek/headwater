package headwater.text;

import com.google.common.collect.Sets;
import headwater.bitmap.BitmapFactory;
import headwater.bitmap.Utils;
import headwater.hash.BitHashableKey;
import headwater.hash.FunnelHasher;
import headwater.data.DataAccess;
import headwater.hash.Hashers;
import headwater.data.MemoryDataAccess;
import headwater.index.BitmapIndex;
import headwater.index.Filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TrigramIndex<K, F>  {
    
    private BitmapIndex<K, F, Trigram> index;
    private DataAccess<K, F, String> observer = new MemoryDataAccess<K, F, String>();
    private FunnelHasher<K> keyHasher;
    
    public TrigramIndex(FunnelHasher<K> keyHasher, FunnelHasher<F> fieldHasher, long trigramHashBitLength) {
        index = new BitmapIndex<K, F, Trigram>(keyHasher, fieldHasher, Hashers.makeHasher(Trigram.class, trigramHashBitLength));
        this.keyHasher = keyHasher;
    }
    
    public TrigramIndex<K, F> withBitmapFactory(BitmapFactory factory) {
        index = index.withBitmapFactory(factory);
        return this;
    }
    
    public TrigramIndex<K, F> withIndexObserver(DataAccess<K, F, String> observer) {
        this.observer = observer;
        return this;
    }
    
    public void add(K key, F field, String value) {
        BitHashableKey<K> bitKey = keyHasher.hashableKey(key);
        observer.observe(bitKey, field, value);
        for (Trigram trigram : Trigram.make(value))
            index.add(key, field, trigram);
    }
    
    public Collection<K> globSearch(F field, String query) throws Exception {
        String[] parcels = query.split("\\*", 0);
        Set<Long> candidates = null;
        for (final String parcel : parcels) {
            if (parcel == null || parcel.length() == 0) continue;
            long[] hits;
            if (parcel.length() < Trigram.N) {
                Filter<Trigram> slice = new Filter<Trigram>() {
                    public boolean matches(Trigram trigram) {
                        return trigram.contains(parcel);
                    }
                };
                hits = index.filter(field, slice);
            } else {
                hits = trigramSearch(field, parcel);
            }
            if (candidates == null)
                candidates = hashSetFrom(hits);
            else
                candidates = Sets.intersection(candidates, hashSetFrom(hits));
            if (candidates.size() == 0)
                break;
        }
        
        
        Set<K> keyCandidates = new HashSet<K>(observer.toKeys(Utils.unbox(candidates.toArray(new Long[candidates.size()]))));
        List<K> results = new ArrayList<K>();
        
        String regexQuery = ".*" + query.replace("*", ".*");
        for (K key: keyCandidates) {
            String value = observer.lookup(key, field);// todo: here.
            if (value.matches(regexQuery))
                results.add(key);
        }
            
                
        return results;
    }
    
    private long[] trigramSearch(F field, String parcel) throws Exception {
        Set<Long> candidates = new HashSet<Long>();
        for (Trigram trigram : Trigram.make(parcel)) {
            for (long hit : index.filter(field, trigram)) {
                candidates.add(hit);
            }
        }
        return Utils.unbox(candidates.toArray(new Long[candidates.size()]));
    }
   
    Set<Long> hashSetFrom(long[] longs) {
        Set<Long> set = new HashSet<Long>();
        for (long l : longs)
            set.add(l);
        return set;
    }
}