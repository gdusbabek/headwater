package headwater.text;

import com.yammer.metrics.Metrics;
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
import java.util.concurrent.TimeUnit;

public class BareIOTrigramWriter<K, F> implements ITrigramWriter<K, F> {
    private static final Timer segmentLookupTimer = Metrics.newTimer(BareIOTrigramWriter.class, "segment_lookup", "trigram", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    private static final Timer segmentSetTimer = Metrics.newTimer(BareIOTrigramWriter.class, "segment_set", "trigram", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    private static final Timer observeTimer = Metrics.newTimer(BareIOTrigramWriter.class, "observing", "trigram", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    private static final Timer indexSerializeTimer = Metrics.newTimer(BareIOTrigramWriter.class, "serialization", "trigram", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    
    private final BigInteger numBits;
    private final int segmentBitLength;
    
    private IO io;
    private KeyObserver<K, F, String> observer;
    
    public BareIOTrigramWriter(long numBits, int segmentBitLength) {
        this.numBits = new BigInteger(Long.toString(numBits));
        this.segmentBitLength = segmentBitLength;
        
        this.io = new FakeCassandraIO();
        observer = new MemLookupObserver<K, F, String>();
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

        TimerContext serializeContext = indexSerializeTimer.time();
        // compute the bit to assert and the index row key.
        final BitHashableKey<K> keyHash = ((FunnelHasher<K>) Hashers.makeHasher(key.getClass(), numBits.longValue())).hashableKey(key); 
        final long segment = keyHash.getHashBit() / segmentBitLength;
        final long bitInSegment = keyHash.getHashBit() % segmentBitLength;
        serializeContext.stop();
        
        TimerContext observeContext = observeTimer.time();
        observer.observe(keyHash, field, value);
        observeContext.stop();
        
        // now assert that bit for each trigram we are indexing.
        
        TimerContext getSegmentContext;
        TimerContext segmentSetContext;
        for (Trigram trigram :Trigram.make(value)) {
            byte[] indexKey = Util.computeIndexRowKey(field, trigram);
            getSegmentContext = segmentLookupTimer.time();
            IBitmap segmentMap = getSegment(indexKey, segment);
            getSegmentContext.stop();
            segmentSetContext = segmentSetTimer.time();
            segmentMap.set(bitInSegment, true);  
            segmentSetContext.stop();
        }
    }
    
    private IBitmap getSegment(byte[] rowKey, long segment) {
        return new IOBitmapSegment(segmentBitLength, io)
                .withRowKey(rowKey)
                .withColName(Utils.longToBytes(segment));
    }
}
