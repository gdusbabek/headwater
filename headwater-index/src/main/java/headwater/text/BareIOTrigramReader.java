package headwater.text;

import com.google.common.collect.Sets;
import headwater.data.ColumnObserver;
import headwater.data.FakeCassandraIO;
import headwater.data.IO;
import headwater.data.Lookup;
import headwater.data.MemLookupObserver;
import headwater.util.Utils;

import java.io.IOError;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BareIOTrigramReader<K, F> implements ITrigramReader<K, F> {

    
    private final BigInteger numBits;
    private final int segmentBitLength;
    
    private IO io;
    private Lookup<K, F, String> lookup;
    
    public BareIOTrigramReader(long numBits, int segmentBitLength) {
        this.numBits = new BigInteger(Long.toString(numBits));
        this.segmentBitLength = segmentBitLength;
        
        // mainly for testing.
        this.io = new FakeCassandraIO();
        MemLookupObserver<K, F, String> dataAccess = new MemLookupObserver<K, F, String>();
        this.lookup = dataAccess;
    }
    
    public BareIOTrigramReader<K, F> withIO(IO io) {
        this.io = io;
        return this;
    }
    
    public BareIOTrigramReader<K, F> withLookup(Lookup<K, F, String> lookup) {
        this.lookup = lookup;
        return this;
    }
    
    public Collection<K> globSearch(F field, String query) {
        String[] parcels = query.split("\\*", 0);
        Set<Long> candidates = null;
        for (final String parcel : parcels) {
            if (parcel == null || parcel.length() == 0) continue;
            
            long [] hits = trigramSearch(field, parcel, new AsciiAugmentationStrategy());
            
            if (candidates == null)
                candidates = Util.hashSetFrom(hits);
            else
                candidates = Sets.intersection(candidates, Util.hashSetFrom(hits));
            
            // if there are no candidates, subsequent intersections with the null set will return the null set.
            if (candidates.size() == 0)
                break;
        }
        
        
        List<K> results = new ArrayList<K>();
        
        if (candidates == null)
            return results; // nothing.
        
        Set<K> keyCandidates = new HashSet<K>(lookup.toKeys(Utils.unbox(candidates.toArray(new Long[candidates.size()]))));
        
        // the candidates may or may not match. the whole point of the bitmap index is to whittle that question down
        // to candidates that we can run the regex against on a single machine.  This is what we do now.
        String regexQuery = query.replace("*", ".*");
        for (K key: keyCandidates) {
            String value = lookup.lookup(key, field);
            if (value != null && value.matches(regexQuery))
                results.add(key);
            }
        
        return results;
    }
    
    private long[] trigramSearch(F field, String parcel, AugmentationStrategy augmentationStrategy) {
        final Set<Long> candidates = new HashSet<Long>();
        for (Trigram trigram :Trigram.make(parcel, augmentationStrategy)) {
            final byte[] indexKey = Util.computeIndexRowKey(field, trigram);
            try {
                io.visitAllColumns(indexKey, 64, new ColumnObserver() {
                    public void observe(byte[] row, byte[] col, byte[] value) {
                        long segment = Utils.bytesToLong(col);
                        long[] assertedInSegment = IOBitmapSegment.getAsserted(value); //getSegment(indexKey, segment).getAsserted();
                        for (long l : assertedInSegment)
                            candidates.add(segment * segmentBitLength + l);
                    }
                });
            } catch (Exception ex) {
                throw new IOError(ex);
            }
        }
        
        return Utils.unbox(candidates.toArray(new Long[candidates.size()]));
    }
}
