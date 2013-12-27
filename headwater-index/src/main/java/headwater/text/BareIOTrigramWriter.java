package headwater.text;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import headwater.bitmap.IBitmap;
import headwater.data.FakeCassandraIO;
import headwater.data.IO;
import headwater.data.KeyObserver;
import headwater.data.MemLookupObserver;
import headwater.hash.BitHashableKey;
import headwater.hash.FunnelHasher;
import headwater.hash.Hashers;
import headwater.util.Utils;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class BareIOTrigramWriter<K, F> implements ITrigramWriter<K, F> {
    private static final Timer addTimer = Metrics.newTimer(BareIOTrigramWriter.class, "index_add", "trigram", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    private static final Meter segmentEvictions = Metrics.newMeter(BareIOTrigramWriter.class, "segment_evictions", "trigram", "meter", TimeUnit.SECONDS);
    
    private final BigInteger numBits;
    private final int segmentBitLength;
    
    private IO io;
    private KeyObserver<K, F, String> observer;
    
    private final LoadingCache<BitmapCacheKey, IBitmap> segmentCache;
    
    public BareIOTrigramWriter(long numBits, final int segmentBitLength, long cacheMemSize) {
        this.numBits = new BigInteger(Long.toString(numBits));
        this.segmentBitLength = segmentBitLength;
        
        this.io = new FakeCassandraIO();
        observer = new MemLookupObserver<K, F, String>();
        
        if (cacheMemSize > 0) {
            long numEntries = cacheMemSize / (segmentBitLength / 8);
            segmentCache = CacheBuilder.newBuilder()
                    .concurrencyLevel(5)
                    .maximumSize(numEntries)
                    .removalListener(new RemovalListener<Object, Object>() {
                        public void onRemoval(RemovalNotification<Object, Object> notification) {
                            segmentEvictions.mark();
                        }
                    })
                    .softValues()
                    .recordStats()
                    .build(new CacheLoader<BitmapCacheKey, IBitmap>() {
                        @Override
                        public IBitmap load(BitmapCacheKey key) throws Exception {
                            return new IOBitmapSegment(segmentBitLength, io)
                                    .withRowKey(key.rowKey)
                                    .withColName(key.colName)
                                    .withReadCaching()
                                    .withAsynchronousUpdates();
                        }
                    });
            segmentCache.stats();
        } else {
            segmentCache = null;
        }
    }
    
    public BareIOTrigramWriter<K, F> withIO(IO io) {
        this.io = io;
        return this;
    }
    
    public BareIOTrigramWriter<K, F> withObserver(KeyObserver<K, F, String> observer) {
        this.observer = observer;
        return this;
    }
    
    public void add(K key, F field, String value) {
        TimerContext ctx = addTimer.time();
        // compute the bit to assert and the index row key.
        final BitHashableKey<K> keyHash = ((FunnelHasher<K>) Hashers.makeHasher(key.getClass(), numBits.longValue())).hashableKey(key); 
        final long segment = keyHash.getHashBit() / segmentBitLength;
        final long bitInSegment = keyHash.getHashBit() % segmentBitLength;
        
        observer.observe(keyHash, field, value);
        
        // now assert that bit for each trigram we are indexing.
        
        for (Trigram trigram :Trigram.make(value)) {
            byte[] indexKey = Util.computeIndexRowKey(field, trigram);
            IBitmap segmentMap = getSegment(indexKey, segment);
            segmentMap.set(bitInSegment, true);  
        }
        ctx.stop();
    }
    
    private IBitmap getSegment(byte[] rowKey, long segment) {
        if (segmentCache == null) {
            return new IOBitmapSegment(segmentBitLength, io)
                    .withRowKey(rowKey)
                    .withColName(Utils.longToBytes(segment));
        } else {
            return segmentCache.getUnchecked(new BitmapCacheKey(rowKey, segment));
        }
    }
    
    private static class BitmapCacheKey {
        private byte[] rowKey;
        private byte[] colName;
        private Integer hashCode;
        
        public BitmapCacheKey(byte[] rowKey, long segment) {
            this.rowKey = rowKey;
            this.colName = Utils.longToBytes(segment);
            
        }

        @Override
        public int hashCode() {
            if (hashCode == null)
                hashCode = Arrays.hashCode(rowKey) ^ Arrays.hashCode(colName);
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof BitmapCacheKey))
                return false;
            return hashCode() == obj.hashCode();
        }
    }
}
