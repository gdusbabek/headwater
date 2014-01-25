package headwater.io;

import com.google.common.primitives.UnsignedBytes;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import headwater.Utils;
import headwater.bitmap.BitmapFactory;
import headwater.bitmap.BitmapUtils;
import headwater.bitmap.IBitmap;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MemoryBitmapIO implements IO<Long, IBitmap> {
    
    private Map<byte[], Map<Long, IBitmap>> data = new TreeMap<byte[], Map<Long, IBitmap>>(UnsignedBytes.lexicographicalComparator());
    private BitmapFactory bitmapFactory = null;
    
    public MemoryBitmapIO() {}
        
    private Map<Long, IBitmap> getRow(byte[] key) {
        synchronized (data) {
            Map<Long, IBitmap> cols = data.get(key);
            if (cols == null) {
                cols = new TreeMap<Long, IBitmap>();
                data.put(key, cols);
            }
            return cols;
        }
    }
    
    public MemoryBitmapIO withBitmapFactory(BitmapFactory factory) {
        this.bitmapFactory = factory;
        return this;
    }
    
    public void put(byte[] key, Long col, IBitmap value) throws Exception {
        getRow(key).put(col, value);
    }

    public IBitmap get(byte[] key, Long col) throws Exception {
        IBitmap value = getRow(key).get(col);
        if (value != null)
            return value;
        if (bitmapFactory == null)
            throw new NotFoundException("Not found!");
        
        // generate an emtpy bitmap and return it.
        value = bitmapFactory.make();
        getRow(key).put(col, value);
        return value;
    }

    public void bulkPut(Map<byte[], List<Tuple<byte[], byte[]>>> data) {
        
    }

    public void visitAllColumns(byte[] key, int pageSize, ColumnObserver observer) throws Exception {
        for (Map.Entry<Long, IBitmap> entry : getRow(key).entrySet()) {
            observer.observe(key, entry.getKey(), entry.getValue());
        }
    }

    public void del(byte[] key, Long col) throws Exception {
        getRow(key).remove(Utils.longToBytes(col));
    }
    
    public int getRowCountUnsafe() {
        return data.size();
    }
    
    // this is basically an OR operation on all common bitsets. Afterward, we get rid of everything.
    // todo: think about concurrency. we'll want to be able to put while we are flushing.
    public void flush(CassandraBitmapIO receiver) throws Exception {
        // first, merge.
        for (byte[] key : data.keySet()) {
            for (Map.Entry<Long, IBitmap> col : data.get(key).entrySet()) {
                try {
                    IBitmap currentValue = receiver.get(key, col.getKey());
                    BitmapUtils.mutatingOR(col.getValue(), currentValue);
                    
                } catch (NotFoundException ex) {
                    // no data in the db. will just use what we have here.
                }
            }
        }
        
        // send it all to the new database.
        receiver.flush(data);
        
        // reset.
        data.clear();
    }
}
