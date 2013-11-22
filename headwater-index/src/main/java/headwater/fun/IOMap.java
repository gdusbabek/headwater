package headwater.fun;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import headwater.data.CassSerializers;
import headwater.data.ColumnObserver;
import headwater.data.IO;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Naive map implementation backed by a row of columns (cassandra)  This is just for fun.
 * 
 * @param <K> must be hashable and comparable!
 * @param <V> can be just about anything.
 */
public class IOMap<K extends Comparable<K>, V> implements Map<K, V> {
    
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128(63342632);
    private static final byte[] SIZE = "size".getBytes();
    
    private final IO io;
    private final byte[] mapRowKey;
    private final byte[] mapMetaRowKey;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    
    public IOMap(byte[] mapRowKey, Class<K> mapKeyType, Class<V> mapValueType, IO io) {
        this.io = io;
        this.mapRowKey = mapRowKey;
        this.keySerializer = CassSerializers.serializerFor(mapKeyType);
        this.valueSerializer = CassSerializers.serializerFor(mapValueType);
        
        mapMetaRowKey = repeatableRandom(mapRowKey, 16);
    }
    
    //
    // Map interface
    //


    public int size() {
        try {
            return CassSerializers.INTEGER_SERIALIZER.fromBytes(io.get(mapMetaRowKey, SIZE));
        } catch (NotFoundException ex) {
            return 0;
        } catch (Exception ex) {
            throw new InternalError("Unexpected data read error");
        }
    }

    public boolean isEmpty() {
        return size() > 0;
    }

    public boolean containsKey(Object key) {
        try {
            io.get(mapRowKey, ((Serializer)keySerializer).toBytes(key));
            return true;
        } catch (ClassCastException ex) {
            return false;
        } catch (NotFoundException ex) {
            return false;
        } catch (Exception ex) {
            throw new InternalError("Unexpected data read error");
        }
    }

    public boolean containsValue(Object value) {
        final AtomicBoolean contains = new AtomicBoolean(false);
        try {
            io.visitAllColumns(mapRowKey, 2048, new ColumnObserver() {
                public void observe(byte[] row, byte[] col, byte[] value) {
                    try {
                        if (valueSerializer.fromBytes(value).equals(value))
                            contains.set(true);
                    } catch (Throwable th) {
                        // swallow it.
                        // this may include ClassCastExceptions.
                    }
                }
            });
        } catch (Exception ex) {
            throw new InternalError("Unexpected data read error");
        }
        return contains.get();
    }

    public V get(Object key) {
        try {
            return valueSerializer.fromBytes(io.get(mapRowKey, ((Serializer)keySerializer).toBytes(key)));
        } catch (NotFoundException ex) {
            return null;
        } catch (Exception ex) {
            throw new InternalError("Unexpected data read error");
        }
    }

    public V put(K key, V value) {
        boolean contains = containsKey(key);
        try {
            io.put(mapRowKey, keySerializer.toBytes(key), valueSerializer.toBytes(value));
            if (!contains) {
                updateSize(size() + 1);
            }
        } catch (Exception ex) {
            throw new InternalError("Unexpeted data read/write error");
        }
        return value;
    }

    public V remove(Object key) {
        V value = null;
        try {
            value = get(key);
            if (value != null) {
                io.del(mapRowKey, ((Serializer)keySerializer).toBytes(key));
                updateSize(size() - 1);
            }
            
        } catch (Exception ex) {
            throw new InternalError("Unexpected data write error");
        }
        return value;
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K,? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    public void clear() {
        for (K key : keySet())
            remove(key);
    }

    public Set<K> keySet() {
        final Set<K> keys = new TreeSet<K>();
        try {
            io.visitAllColumns(mapRowKey, 2048, new ColumnObserver() {
                public void observe(byte[] row, byte[] col, byte[] value) {
                    keys.add(keySerializer.fromBytes(col));
                }
            });
        } catch (Exception ex) {
            throw new InternalError("Unexpected data read error");
        }
        return keys;
    }

    public Collection<V> values() {
        final Set<V> values = new HashSet<V>();
        try {
            io.visitAllColumns(mapRowKey, 2048, new ColumnObserver() {
                public void observe(byte[] row, byte[] col, byte[] value) {
                    values.add(valueSerializer.fromBytes(value));
                }
            });
        } catch (Exception ex) {
            throw new InternalError("Unexpected data read error");
        }
        return values;
    }

    public Set<Entry<K, V>> entrySet() {
        final Set<Entry<K, V>> entries = new HashSet<Entry<K, V>>();
        try {
            io.visitAllColumns(mapRowKey, 2048, new ColumnObserver() {
                public void observe(byte[] row, byte[] col, byte[] value) {
                    entries.add(new CassMapEntry(keySerializer.fromBytes(col), valueSerializer.fromBytes(value)));
                }
            });
        } catch (Exception ex) {
            throw new InternalError("Unexpected data read error");
        }
        return entries;
    }
    
    //
    // helpers.
    //
    
    private void updateSize(int size) throws Exception {
        io.put(mapMetaRowKey, SIZE, CassSerializers.INTEGER_SERIALIZER.toBytes(size));
    }
    
    private class CassMapEntry implements Entry<K, V> {
        private final K key;
        private V value;
        
        CassMapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        public V setValue(V value) {
            if (value == null) throw new NullPointerException("null is not allowed in Map entries");
            this.value = value;
            return this.value;
        }
    }
    
    private byte[] repeatableRandom(byte[] seed, int appendLength) {
        byte[] buf = new byte[seed.length + appendLength];
        byte[] append = HASH_FUNCTION.newHasher().putBytes(seed).hash().asBytes();
        System.arraycopy(seed, 0, buf, 0, seed.length);
        System.arraycopy(append, 0, buf, seed.length, append.length);
        return buf;
    }
}
