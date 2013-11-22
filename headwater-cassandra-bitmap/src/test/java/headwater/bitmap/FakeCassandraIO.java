package headwater.bitmap;

import com.google.common.primitives.UnsignedBytes;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import headwater.data.ColumnObserver;
import headwater.data.IO;

import java.util.Map;
import java.util.TreeMap;

public class FakeCassandraIO implements IO {
    
    Map<byte[], Map<byte[], byte[]>> data = new TreeMap<byte[], Map<byte[], byte[]>>(UnsignedBytes.lexicographicalComparator());
    
    private Map<byte[], byte[]> getRow(byte[] key) {
        synchronized (data) {
            Map<byte[], byte[]> cols = data.get(key);
            if (cols == null) {
                cols = new TreeMap<byte[], byte[]>(UnsignedBytes.lexicographicalComparator());
                data.put(key, cols);
            }
            return cols;
        }
    }
    
    public void put(byte[] key, byte[] col, byte[] value) throws Exception {
        // we copy because we don't want anybody with a pointer to mess with data "written" to the database.
        byte[] colCopy = new byte[col.length];
        byte[] valueCopy = new byte[value.length];
        System.arraycopy(col, 0, colCopy, 0, col.length);
        System.arraycopy(value, 0, valueCopy, 0, value.length);
        getRow(key).put(colCopy, valueCopy);
    }

    public byte[] get(byte[] key, byte[] col) throws Exception {
        byte[] value = getRow(key).get(col);
        if (value == null)
            throw new NotFoundException("Not found!");
        else
            return value;
    }

    public void visitAllColumns(byte[] key, int pageSize, ColumnObserver observer) throws Exception {
        for (Map.Entry<byte[], byte[]> entry : getRow(key).entrySet()) {
            observer.observe(key, entry.getKey(), entry.getValue());
        }
    }

    public void del(byte[] key, byte[] col) throws Exception {
        getRow(key).remove(col);
    }
}
