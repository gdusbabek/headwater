package headwater.io;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.primitives.UnsignedBytes;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import headwater.Utils;
import headwater.bitmap.BitmapFactory;
import headwater.bitmap.BitmapUtils;
import headwater.bitmap.IBitmap;

import java.util.Map;
import java.util.TreeMap;

public class MemoryBitmapIO implements IO<Long, IBitmap> {
    private static final Timer mergeTimer = Utils.getMetricRegistry().timer(MetricRegistry.name(MemoryBitmapIO.class, "bitmaps", "merging"));
    private static final Timer mutatingOrTimer = Utils.getMetricRegistry().timer(MetricRegistry.name(MemoryBitmapIO.class, "bitmaps", "OR"));
    private static final Histogram flushBatchSize = Utils.getMetricRegistry().histogram(MetricRegistry.name(MemoryBitmapIO.class, "bitmaps", "batch-size"));
    
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

    public void visitAllColumns(byte[] key, int pageSize, ColumnObserver<Long, IBitmap> observer) throws Exception {
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
        
        Timer.Context mergeCtx = mergeTimer.time();
        Timer.Context morCtx;
        long count = 0;
        
        for (byte[] key : data.keySet()) {
            Map<Long, IBitmap> memory = data.get(key);
            Map<Long, IBitmap> disk = receiver.bulkGet(key, memory.keySet());
            
            // merge.
            for (Map.Entry<Long, IBitmap> diskEntry : disk.entrySet()) {
                morCtx = mutatingOrTimer.time();
                BitmapUtils.mutatingOR(memory.get(diskEntry.getKey()), diskEntry.getValue());
                morCtx.stop();
                count += 1;
            }
        }
        mergeCtx.stop();
        flushBatchSize.update(count);
        
        // send it all to the new database.
        receiver.flush(data);
        
        // reset.
        data.clear();
    }
}
