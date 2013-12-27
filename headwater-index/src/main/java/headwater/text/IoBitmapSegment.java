package headwater.text;

import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import headwater.bitmap.AbstractBitmap;
import headwater.data.IO;
import headwater.util.Utils;

import java.io.IOError;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IOBitmapSegment extends AbstractBitmap {
    
    // used in async update mode.
    private static ExecutorService asyncUpdateService = Executors.newFixedThreadPool(10, new ThreadFactory() {
        private final AtomicInteger threadNumber = new AtomicInteger(0);
        private final ThreadGroup group = Thread.currentThread().getThreadGroup();
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                                  "AsyncBitmapUpdate-" + threadNumber.getAndIncrement(),
                                  0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
        
    });
    
    private static Counter readHits = Metrics.newCounter(IOBitmapSegment.class, "segment_hits", "trigram");
    private static Counter readMissses = Metrics.newCounter(IOBitmapSegment.class, "segment_misses", "trigram");
    private static Timer readTimer = Metrics.newTimer(IOBitmapSegment.class, "segment_reads", "trigram", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    private static Timer writeTimer = Metrics.newTimer(IOBitmapSegment.class, "segment_writes", "trigram", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    private static Counter skippedWrites = Metrics.newCounter(IOBitmapSegment.class, "skipped_writes", "trigram");
    
    protected byte[] rowKey;
    protected byte[] colName;
    
    private final int bitLength;
    private IO io;
    
    private boolean cacheing = false;
    private UpdateContext updateContext = null;
    
    public IOBitmapSegment(int bitLength, IO io) {
        this.bitLength = bitLength;
        this.io = io;
    }
    
    public IOBitmapSegment withRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
        return this;
    }
    
    public IOBitmapSegment withColName(byte[] colName) {
        this.colName = colName;
        return this;
    }
    
    public IOBitmapSegment withReadCaching() {
        cacheing = true;
        return this;
    }
    
    public IOBitmapSegment withAsynchronousUpdates() {
        this.updateContext = new UpdateContext();
        return this;
    }
    
    public int hashCode() {
        return Arrays.hashCode(rowKey) ^ Arrays.hashCode(colName);
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
    
    public static long[] getAsserted(byte[] curValue) {
        final List<Long> asserted = new ArrayList<Long>();
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

    public long[] getAsserted() {
        return IOBitmapSegment.getAsserted(getCurrentValue());
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
            Util.flipOn(curValue, (bit));
        else
            Util.flipOff(curValue, (bit));
        
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
        if (updateContext != null)
            asyncUdate(buf);        
        else
            syncUpdate(buf);
    }
    
    
    private void asyncUdate(final byte[] buf) {
        updateContext.increment();
        asyncUpdateService.submit(new Runnable() {
            public void run() {
                int value = updateContext.decrement();
                if (value == 0) {
                    syncUpdate(buf);
                } else {
                    skippedWrites.inc();
                }
            }
        });
    }
    
    private void syncUpdate(byte[] buf) {
        TimerContext ctx = writeTimer.time();
        try {
            io.put(rowKey, colName, buf);
        } catch (Exception ex) {
            throw new IOError(ex);
        } finally {
            ctx.stop();
        }
    }
    
    
    private byte[] cachedCurrentValue = null;
    
    
    byte[] getCurrentValue() {
        if (cacheing) {
            if (cachedCurrentValue == null) {
                readMissses.inc();
                cachedCurrentValue = getUncachedCurrentValue();
            } else {
                readHits.inc();
            }
            return cachedCurrentValue;
        } else {
            return getUncachedCurrentValue();
        }
    }
    
    byte[] getUncachedCurrentValue() {
        TimerContext ctx = readTimer.time();
        try {
            return io.get(rowKey, colName);
        } catch (NotFoundException ex) {
            return new byte[bitLength];
        } catch (Exception ex) {
            throw new IOError(ex);
        } finally {
            ctx.stop();
        }
    }
    
    
    private class UpdateContext {
        private AtomicInteger count = new AtomicInteger(0);
        
        public void increment() {
            count.incrementAndGet();
        } 
        
        public int decrement() {
            return count.decrementAndGet();
        }
    }
} 