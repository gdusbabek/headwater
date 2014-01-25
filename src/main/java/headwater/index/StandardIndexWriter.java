package headwater.index;

import headwater.bitmap.BitmapFactory;
import headwater.bitmap.IBitmap;
import headwater.bitmap.MemoryBitmap2;
import headwater.hashing.BitHashableKey;
import headwater.hashing.FunnelHasher;
import headwater.hashing.Hashers;
import headwater.io.IO;
import headwater.io.MemoryBitmapIO;
import headwater.trigram.Trigram;

import java.math.BigInteger;

public class StandardIndexWriter<K, F> implements IndexWriter<K, F, String> {
    
    private final int segmentBitLength;
    private final BigInteger indexBitLength;
    private KeyObserver<K, F, String> observer = new NullKeyObserver<K, F, String>();
    
    private IO<IBitmap> io;
    
    public StandardIndexWriter(final int segmentBitLength, long indexBitLength) {
        if (indexBitLength % segmentBitLength != 0)
            throw new Error("indexBitLength must be evenly divisible by segmentBitLength");
        
        this.segmentBitLength = segmentBitLength;
        this.indexBitLength = new BigInteger(Long.toString(indexBitLength));
        this.io = new MemoryBitmapIO().withBitmapFactory(new BitmapFactory() {
            public IBitmap make() {
                return new MemoryBitmap2(segmentBitLength);
            }
        });
    }
    
    public StandardIndexWriter<K, F> withObserver(KeyObserver<K, F, String> observer) {
        this.observer = observer;
        return this;
    }
    
    public StandardIndexWriter<K, F> withIO(IO io) {
        this.io = io;
        return this;
    }
    
    public void add(K key, F field, String value) {
        final BitHashableKey<K> keyHash = ((FunnelHasher<K>) Hashers.makeHasher(key.getClass(), indexBitLength.longValue())).hashableKey(key);
        final long segment = keyHash.getHashBit() / segmentBitLength;
        final long bitInSegment = keyHash.getHashBit() % segmentBitLength;
        
        observer.observe(keyHash, field, value);
        
        // now assert that bit for each trigram we are indexing.
        
        for (Trigram trigram :Trigram.make(value.toString())) {
            byte[] indexKey = Hashers.computeIndexRowKey(field, trigram);
            //IBitmap segmentMap = getSegment(indexKey, segment);
            try {
                IBitmap segmentMap = io.get(indexKey, segment);
                segmentMap.set(bitInSegment, true);  
            } catch (Exception ex) {
                // shouldn't happen because NotFoundException is already dealt with.
                throw new Error(ex);
            }
        }
    }
}
