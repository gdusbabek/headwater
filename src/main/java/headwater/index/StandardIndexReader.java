package headwater.index;

import com.google.common.collect.Sets;
import headwater.Utils;
import headwater.bitmap.IBitmap;
import headwater.hashing.Hashers;
import headwater.io.ColumnObserver;
import headwater.io.IO;
import headwater.io.MemoryBitmapIO;
import headwater.trigram.AsciiAugmentationStrategy;
import headwater.trigram.AugmentationStrategy;
import headwater.trigram.Trigram;
import org.apache.oro.text.GlobCompiler;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StandardIndexReader<K, F> implements IndexReader<K, F, String> {
    private static final Logger log = LoggerFactory.getLogger(StandardIndexReader.class);
    
    private IO<Long, IBitmap> io;
    private final int segmentBitLength;
    private DataLookup<K, F, String> dataLookup;
    private KeyLookup<K> keyLookup;
    
    public StandardIndexReader(int segmentBitLength) {
        this.segmentBitLength = segmentBitLength;
        this.io = new MemoryBitmapIO();
    }
    
    public StandardIndexReader<K, F> withIO(IO<Long, IBitmap> io) {
        this.io = io;
        return this;
    }
    
    public StandardIndexReader<K, F> withDataLookup(DataLookup<K, F, String> lookup) {
        this.dataLookup = lookup;
        return this;
    }
    
    public StandardIndexReader<K, F> withKeyLookup(KeyLookup<K> lookup) {
        this.keyLookup = lookup;
        return this;
    }
    
    // todo: consider the query string "*abcdef*". Right now we are intersecting all the trigrams (abc, bcd, cde, def).
    // but we could get away with intersecing (abc, def) and have the same results.
    public Collection<K> globSearch(F field, String valueQuery) {
        long queryStart = System.currentTimeMillis();
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
        long queryEnd = System.currentTimeMillis();
        
        List<K> results = new ArrayList<K>();
        
        if (candidateBits == null)
            return results; // nothing.
        
        Set<K> keyCandidates = new HashSet<K>(keyLookup.toKeys(Utils.unbox(candidateBits.toArray(new Long[candidateBits.size()]))));
        
        // the candidates may or may not match. the whole point of the bitmap index is to whittle that question down
        // to candidates that we can run the regex against on a single machine.  This is what we do now.

        long pareStart = System.currentTimeMillis();
        GlobCompiler gc = new GlobCompiler();
        Pattern pattern = null;
        try {
            pattern = gc.compile(valueQuery);
        } catch (MalformedPatternException ex) {
            throw new IOError(ex);
        }
        PatternMatcher matcher = new Perl5Matcher();
        
        for (K key: keyCandidates) {
            String value = dataLookup.lookup(key, field);
            if (value != null && matcher.matches(value, pattern))
                results.add(key);
        }
        long pareEnd = System.currentTimeMillis();

//        System.out.println(String.format("c:%d q:%d p:%d", keyCandidates.size(), queryEnd-queryStart, pareEnd-pareStart));
        
        return results;
    }
    
    // *c*b* -> .*c.*b.*
    
    private long[] trigramSearch(F field, String parcel, AugmentationStrategy augmentationStrategy) {
        final Set<Long> candidates = new HashSet<Long>();
        final UnsafeCounter colCount = new UnsafeCounter();
        for (Trigram trigram :Trigram.makeNonOverlapping(parcel, augmentationStrategy)) {
            final byte[] indexKey = Hashers.computeIndexRowKey(field, trigram);
            try {
                io.visitAllColumns(indexKey, 64, new ColumnObserver<Long, IBitmap>() {
                    public void observe(byte[] row, Long segment, IBitmap value) {
                        colCount.inc();
                        long[] assertedInSegment = value.getAsserted();
                        for (long l : assertedInSegment)
                            candidates.add(segment * segmentBitLength + l);
                    }
                });
            } catch (Exception ex) {
                throw new IOError(ex);
            }
        }
//        System.out.println(String.format("cols: %d", colCount.count));
        return Utils.unbox(candidates.toArray(new Long[candidates.size()]));
    }
    
    private class UnsafeCounter {
        private int count = 0;
        void inc() {
            count += 1;
        }
        
    }
}
