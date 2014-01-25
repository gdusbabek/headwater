package headwater.index;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.LongSerializer;
import headwater.hashing.BitHashableKey;
import headwater.io.BatchIO;
import headwater.io.ColumnObserver;
import headwater.io.IO;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IOJack<K, F, V> implements KeyObserver<K, F, V>, KeyLookup<K>, DataLookup<K, F, V> {
    private static final long DEFAULT_FLUSH_THRESHOLD = 10000;
    private static final byte[] LONG_ROW_KEY = "BIT_TO_KEY_ROW".getBytes();
    
    private final Map<Long, K> writeCache = new HashMap<Long, K>();
    private static final Serializer<Long> longSerializer = LongSerializer.get();
    private long writeOps = 0;
    private long flushThreshold = DEFAULT_FLUSH_THRESHOLD;
    private boolean isLongRow = true;
    private IO<Long, K> bitIO = null;
    private IO<F, V> dataIO; Serializer<K> keySerializer;
    private boolean isBatching = false;
    
    public IOJack(Serializer<K> keySerializer) {
        withLongRow(defaultBitIO(writeCache));
        withData(defaultDataIO(), keySerializer);
    }
    
    public IOJack<K,F,V> withFlushThreshold(long flushThreshold) {
        this.flushThreshold = flushThreshold;
        return this;
    }
    
    public IOJack<K,F,V> withLongRow(IO<Long,K> io) {
        this.isLongRow = true;
        this.bitIO = io;
        this.isBatching = io instanceof BatchIO;
        return this;
    }
    
    public IOJack<K,F,V> withShortRow(IO<Long,K> io) {
        this.isLongRow = false;
        this.bitIO = io;
        this.isBatching = io instanceof BatchIO;
        return this;
    }
    
    public IOJack<K, F, V> withData(IO<F, V> io, Serializer<K> serializer) {
        this.keySerializer = serializer;
        this.dataIO = io;
        return this;
    }
    
    public Collection<K> toKeys(long[] bits) {
        // todo: this will be better when bulk operations are supported.
        Set<K> keys = new HashSet<K>();
        for (long bit : bits)
            keys.add(toKey(bit));
        return keys;
    }

    public K toKey(long bit) {
        try {
            if (isLongRow)
                return bitIO.get(LONG_ROW_KEY, bit);
            else
                return bitIO.get(longSerializer.toBytes(bit), bit);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public V lookup(K key, F field) {
        try {
            return dataIO.get(keySerializer.toBytes(key), field);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public void observe(BitHashableKey<K> key, F field, V value) {
        writeCache.put(key.getHashBit(), key.getKey());
        writeOps += 1;
        try {
            dataIO.put(keySerializer.toBytes(key.getKey()), field, value);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
        if (isBatching && writeOps > flushThreshold) {
            try {
                flush();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            writeOps = 0;
        }
    }

    private void flush() throws Exception {
        BatchIO<Long, K> io = (BatchIO<Long,K>)this.bitIO;
        io.begin();
        for (Map.Entry<Long, K> entry : writeCache.entrySet()) {
            if (isLongRow)
                io.put(LONG_ROW_KEY, entry.getKey(), entry.getValue());
            else
                io.put(longSerializer.toBytes(entry.getKey()), entry.getKey(), entry.getValue());            
        }
        io.commit();
    }
    
    private IO<Long,K> defaultBitIO(final Map<Long, K> map) {
        return new IO<Long, K>() {
            public void put(byte[] key, Long col, K value) throws Exception {
                // already done.
            }

            public K get(byte[] key, Long col) throws Exception {
                return map.get(col);
            }

            public void del(byte[] key, Long col) throws Exception {
                map.remove(col);
            }

            public void visitAllColumns(byte[] key, int pageSize, ColumnObserver<Long, K> observer) throws Exception {
                for (Map.Entry<Long, K> entry : map.entrySet()) {
                    observer.observe(LONG_ROW_KEY, entry.getKey(), entry.getValue());
                }
            }
        };
    }
    
    // todo: this is where it would be nice if IO were triply generic.
    private IO<F, V> defaultDataIO() {
        final Multimap<Integer, F> fields = HashMultimap.create(); 
        final Map<CompositeKey<Integer,F>, V> store = new HashMap<CompositeKey<Integer,F>, V>();
        
        return new IO<F, V>() {
            public void put(byte[] key, F col, V value) throws Exception {
                int keyHash = Arrays.hashCode(key);
                store.put(new CompositeKey<Integer,F>(keyHash, col), value);
                fields.put(keyHash, col);
            }

            public V get(byte[] key, F col) throws Exception {
                return store.get(new CompositeKey<Integer,F>(Arrays.hashCode(key), col));
            }

            public void del(byte[] key, F col) throws Exception {
                store.remove(new CompositeKey<Integer,F>(Arrays.hashCode(key), col));
            }

            public void visitAllColumns(byte[] key, int pageSize, ColumnObserver<F, V> observer) throws Exception {
                int keyHash = Arrays.hashCode(key);
                for (F field : fields.get(keyHash)) {
                    observer.observe(key, field, store.get(new CompositeKey<Integer,F>(keyHash, field)));
                }
            }
        };
    }
    
    private static class CompositeKey<K, F> {
        private final K k;
        private final F f;
        private final Integer hashCode;
        
        public CompositeKey(K k, F f) {
            this.k = k;
            this.f = f;
            this.hashCode = k.hashCode() ^ f.hashCode();
        }
        
        @Override
        public int hashCode() {
            return hashCode;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof CompositeKey))
                return false;
            CompositeKey other = (CompositeKey)obj;
            return other.k.equals(this.k) && other.f.equals(this.f);
        }
    }
}
