package headwater.index;

import com.google.common.collect.Sets;
import headwater.Utils;
import headwater.bitmap.IBitmap;
import headwater.hashing.Hashers;
import headwater.io.ColumnObserver;
import headwater.io.IO;
import headwater.io.MemoryIO;
import headwater.trigram.AsciiAugmentationStrategy;
import headwater.trigram.AugmentationStrategy;
import headwater.trigram.Trigram;
import org.apache.oro.text.GlobCompiler;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Matcher;

import java.io.IOError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MemoryIndexReader<K, F> implements IndexReader<K, F, String> {
    
    
    private IO io;
    private final int segmentBitLength;
    private Lookup<K, F, String> lookup;
    
    public MemoryIndexReader(int segmentBitLength) {
        this.segmentBitLength = segmentBitLength;
        this.io = new MemoryIO();
    }
    
    public MemoryIndexReader<K, F> withIO(IO io) {
        this.io = io;
        return this;
    }
    
    public MemoryIndexReader<K, F> withLookup(Lookup<K, F, String> lookup) {
        this.lookup = lookup;
        return this;
    }
    
    public Collection<K> globSearch(F field, String valueQuery) {
        String[] parcels = valueQuery.split("\\*", 0);
        Set<Long> candidateBits = null;
        for (final String parcel : parcels) {
            if (parcel == null || parcel.length() == 0) continue;
            
            long[] hits = trigramSearch(field, parcel, new AsciiAugmentationStrategy());
            
            // update candidates.
            if (candidateBits == null)
                candidateBits = Utils.hashSetFrom(hits);
            else
                candidateBits = Sets.intersection(candidateBits, Utils.hashSetFrom(hits));
            
            // if there are no candidates, subsequent intersections with the null set will return the null set.
            if (candidateBits.size() == 0)
                break;
        }
        
        List<K> results = new ArrayList<K>();
        
        if (candidateBits == null)
            return results; // nothing.
        
        Set<K> keyCandidates = new HashSet<K>(lookup.toKeys(Utils.unbox(candidateBits.toArray(new Long[candidateBits.size()]))));
        
        // the candidates may or may not match. the whole point of the bitmap index is to whittle that question down
        // to candidates that we can run the regex against on a single machine.  This is what we do now.

        GlobCompiler gc = new GlobCompiler();
        Pattern pattern = null;
        try {
            pattern = gc.compile(valueQuery);
        } catch (MalformedPatternException ex) {
            throw new IOError(ex);
        }
        PatternMatcher matcher = new Perl5Matcher();
        
        for (K key: keyCandidates) {
            String value = lookup.lookup(key, field);
            if (value != null && matcher.matches(value, pattern))
                results.add(key);
        }
        
        return results;
    }
    
    // *c*b* -> .*c.*b.*
    
    private long[] trigramSearch(F field, String parcel, AugmentationStrategy augmentationStrategy) {
        final Set<Long> candidates = new HashSet<Long>();
        for (Trigram trigram :Trigram.make(parcel, augmentationStrategy)) {
            final byte[] indexKey = Hashers.computeIndexRowKey(field, trigram);
            try {
                io.visitAllColumns(indexKey, 64, new ColumnObserver() {
                    public void observe(byte[] row, long segment, IBitmap value) {
                        long[] assertedInSegment = value.getAsserted();
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
