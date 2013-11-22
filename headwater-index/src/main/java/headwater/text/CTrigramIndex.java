package headwater.text;

import com.google.common.collect.Sets;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import headwater.bitmap.AbstractBitmap;
import headwater.bitmap.IBitmap;
import headwater.util.Utils;
import headwater.data.ColumnObserver;
import headwater.data.IO;
import headwater.data.KeyObserver;
import headwater.data.Lookup;
import headwater.data.MemLookupObserver;
import headwater.hash.BitHashableKey;
import headwater.hash.FunnelHasher;
import headwater.hash.Hashers;

import java.io.IOError;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CTrigramIndex<K, F> implements ITrigramIndex<K, F> {
    
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128(543231);
    
    // number of bits in the entire index (not each segment). this can be a very big number.
    private final BigInteger numBits;
    private final int segmentBitLength;
    
    private IO io;
    
    private KeyObserver<K, F, String> observer;
    private Lookup<K, F, String> lookup;
    
    
    public CTrigramIndex(long numBits, int segmentBitLength) {
        this.numBits = new BigInteger(Long.toString(numBits));
        this.segmentBitLength = segmentBitLength;
        
        MemLookupObserver<K, F, String> dataAccess = new MemLookupObserver<K, F, String>();
        this.observer = dataAccess;
        this.lookup = dataAccess;
    }
    
    public CTrigramIndex<K, F> withIO(IO io) {
        this.io = io;
        return this;
    }
    
    public CTrigramIndex<K, F> withObserver(KeyObserver<K, F, String> observer) {
        this.observer = observer;
        return this;
    }
    
    public CTrigramIndex<K, F> withLookup(Lookup<K, F, String> lookup) {
        this.lookup = lookup;
        return this;
    }
    
    public void add(K key, F field, String value) {
        
        // compute the bit to assert and the index row key.
        final BitHashableKey<K> keyHash = ((FunnelHasher<K>)Hashers.makeHasher(key.getClass(), numBits.longValue())).hashableKey(key); 
        final long segment = keyHash.getHashBit() / segmentBitLength;
        final long bitInSegment = keyHash.getHashBit() % segmentBitLength;
        
        observer.observe(keyHash, field, value);
        
        // now assert that bit for each trigram we are indexing.
        for (Trigram trigram :Trigram.make(value)) {
            byte[] indexKey = computeIndexRowKey(field, trigram);
            getSegment(indexKey, segment).set(bitInSegment);    
        }
    }
    
    public Collection<K> globSearch(F field, String query) {
        String[] parcels = query.split("\\*", 0);
        Set<Long> candidates = null;
        for (final String parcel : parcels) {
            if (parcel == null || parcel.length() == 0) continue;
            
            long [] hits = trigramSearch(field, parcel, new AsciiAugmentationStrategy());
            
            if (candidates == null)
                candidates = hashSetFrom(hits);
            else
                candidates = Sets.intersection(candidates, hashSetFrom(hits));
            
            // if there are no candidates, subsequent intersections with the null set will return the null set.
            if (candidates.size() == 0)
                break;
        }
        
        
        List<K> results = new ArrayList<K>();
        
        if (candidates == null)
            return results; // nothing.
        
        Set<K> keyCandidates = new HashSet<K>(observer.toKeys(Utils.unbox(candidates.toArray(new Long[candidates.size()]))));
        
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
    
    //
    // helpers
    //
    
    private long[] trigramSearch(F field, String parcel, AugmentationStrategy augmentationStrategy) {
        final Set<Long> candidates = new HashSet<Long>();
        for (Trigram trigram :Trigram.make(parcel, augmentationStrategy)) {
            final byte[] indexKey = computeIndexRowKey(field, trigram);
            try {
                io.visitAllColumns(indexKey, 64, new ColumnObserver() {
                    public void observe(byte[] row, byte[] col, byte[] value) {
                        long segment = Utils.bytesToLong(col);
                        IBitmap segmentBitmap = new ReadOnlyCassSegmentBitmap(segmentBitLength, value);
                        long[] assertedInSegment = segmentBitmap.getAsserted(); //getSegment(indexKey, segment).getAsserted();
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
    
    private byte[] computeIndexRowKey(F field, Trigram trigram) {
        Hasher hasher = HASH_FUNCTION.newHasher();
        Funnel<F> fieldFunnel = (Funnel<F>)Hashers.funnelFor(field.getClass());
        Funnel<Trigram> trigramFunnel = Hashers.funnelFor(Trigram.class);
        
        hasher.putObject(field, fieldFunnel);
        hasher.putObject(trigram, trigramFunnel);
        
        HashCode indexKeyHash = hasher.hash();
        byte[] indexKey = indexKeyHash.asBytes();
        return indexKey;
    }
    
    
    private IBitmap getSegment(byte[] rowKey, long segment) {
        return new CassSegmentBitmap(segmentBitLength)
                .withRowKey(rowKey)
                .withColName(Utils.longToBytes(segment));
    }
    
    private class CassSegmentBitmap extends AbstractBitmap {
        
        protected byte[] rowKey;
        protected byte[] colName;
        
        private final int bitLength;
        
        public CassSegmentBitmap(int bitLength) {
            this.bitLength = bitLength;
        }
        
        public CassSegmentBitmap withRowKey(byte[] rowKey) {
            this.rowKey = rowKey;
            return this;
        }
        
        public CassSegmentBitmap withColName(byte[] colName) {
            this.colName = colName;
            return this;
        }
        
        //
        // IBitmap interface
        //
        
        @Override
        public Object clone() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public long getBitLength() {
            return this.bitLength;
        }

        public void set(long bit, boolean value) {
            set((int)bit, value);
        }

        public void set(long... bits) {
            for (long bit : bits)
                set(bit, true);
        }

        public boolean get(long bit) {
            return get((int)bit);
        }

        public long[] getAsserted() {
            final List<Long> asserted = new ArrayList<Long>();
            byte[] curValue = getCurrentValue();
            long bitIndex = 0;
            for (byte b : curValue) {
                for (int i = 0; i < 8; i++) {
                    if ((b & 0x01) == 0x01)
                        asserted.add(bitIndex + i);
                    b >>>= 1;
                }
                bitIndex += 8;
            }
            return Utils.unbox(asserted.toArray(new Long[asserted.size()]));
        }

        public void clear() {
            try {
                io.del(rowKey, colName);
            } catch (Exception ex) {
                throw new IOError(ex);
            }
        }

        public boolean isEmpty() {
            byte[] curValue;
            try {
                curValue = io.get(rowKey, colName);
            } catch (NotFoundException ex) {
                return true;
            } catch (Exception ex) {
                throw new IOError(ex);
            }
            for (byte b : curValue) {
                if (b != 0)
                    return false;
            }
            return true;
        }

        public byte[] toBytes() {
            return getCurrentValue();
        }

        public byte[] toBytes(int byteStart, int numBytes) {
            byte[] currentValue = getCurrentValue();
            byte[] buf = new byte[numBytes];
            System.arraycopy(currentValue, byteStart, buf, 0, numBytes);
            return buf;
        }
        
        //
        // helpers.
        //
        
        public void set(int bit, boolean value) {
            
            // read old value.
            byte[] curValue = getCurrentValue();

            
            // set it.
            if (value)
                flipOn(curValue, (bit));
            else
                flipOff(curValue, (bit));
            
            // write it.
            setCurrentValue(curValue);
        }
        
        public boolean get(int bit) {
            byte[] curValue = getCurrentValue();
            int index = bit % 8;
            int bitInByte = bit % 8;
            return (curValue[index] & (0x01 << bitInByte)) > 0;
        }
        
        private void setCurrentValue(byte[] buf) {
            try {
                io.put(rowKey, colName, buf);
            } catch (Exception ex) {
                throw new IOError(ex);
            }
        }
        
        byte[] getCurrentValue() {
            try {
                return io.get(rowKey, colName);
            } catch (NotFoundException ex) {
                return new byte[bitLength];
            } catch (Exception ex) {
                throw new IOError(ex);
            }
        } 
    }
    
    private class ReadOnlyCassSegmentBitmap extends CassSegmentBitmap {
        
        private final byte[] bitmap;
        public ReadOnlyCassSegmentBitmap(int bitLength, byte[] bitmap) {
            super(bitLength);
            this.bitmap = bitmap;
        }

        @Override
        byte[] getCurrentValue() {
            return bitmap;
        }
    }
    
    private static void flipOn(byte[] buf, int bit) {
        int index = bit / 8;
        int bitInByte = bit % 8;
        buf[index] |= 0x01 << bitInByte;
    }
    
    private static void flipOff(byte[] buf, int bit) {
        int index = bit / 8;
        int bitInByte = bit % 8;
        buf[index] &= ~(0x01 << bitInByte);
    }
    
    
    private static Set<Long> hashSetFrom(long[] longs) {
        Set<Long> set = new HashSet<Long>();
        for (long l : longs)
            set.add(l);
        return set;
    }
    
    
    
}
