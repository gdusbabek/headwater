package headwater;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class Utils {
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final JmxReporter metricsReporter = JmxReporter.forRegistry(metricRegistry).build();
    
    static {
        metricsReporter.start();
        Runtime.getRuntime().addShutdownHook(new Thread("Stop-Jmx-Reporter") {
            @Override
            public void run() {
                try {
                    metricsReporter.stop();
                } catch (Throwable ex) {
                    ex.printStackTrace(System.err);
                }
            }
        });
    }
    
    public static MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }
    
    public static long[] unbox(Long[] arr) {
        long[] newarr = new long[arr.length];
        for (int i = 0; i < newarr.length; i++)
            newarr[i] = arr[i];
        return newarr;
    }
    
    public static int[] unbox(Integer[] arr) {
        int[] newarr = new int[arr.length];
        for (int i = 0; i < newarr.length; i++)
            newarr[i] = arr[i];
        return newarr;
    }
    
    public static long bytesToLong(byte[] buf) {
        return (((long)buf[0] << 56) +
                ((long)(buf[1] & 255) << 48) +
                ((long)(buf[2] & 255) << 40) +
                ((long)(buf[3] & 255) << 32) +
                ((long)(buf[4] & 255) << 24) +
                ((buf[5] & 255) << 16) + 
                ((buf[6] & 255) << 8) +
                ((buf[7] & 255) << 0));
    }
    
    public static byte[] longToBytes(long l) {
        byte[] buf = new byte[8];
        buf[0] = (byte)(l >>> 56);
        buf[1] = (byte)(l >>> 48);
        buf[2] = (byte)(l >>> 40);
        buf[3] = (byte)(l >>> 32);
        buf[4] = (byte)(l >>> 24);
        buf[5] = (byte)(l >>> 16);
        buf[6] = (byte)(l >>> 8);
        buf[7] = (byte)(l >>> 0);
        return buf;
    }
    
    public static void flipOn(byte[] buf, long bit) {
        int index = (int)(bit / 8L);
        int bitInByte = (int)(bit % 8L);
        buf[index] |= 0x01 << bitInByte;
    }
    
    public static void flipOff(byte[] buf, long bit) {
        int index = (int)(bit / 8L);
        int bitInByte = (int)(bit % 8L);
        buf[index] &= ~(0x01 << bitInByte);
    }
    
    public static boolean isAsserted(byte[] buf, long bit) {
        int index = (int)(bit / 8L);
        int bitInByte = (int)(bit % 8L);  
        return (buf[index] & (0x01 << bitInByte)) > 0;
    }
    
    public static long[] getAsserted(byte[] buf) {
        final List<Long> asserted = new ArrayList<Long>();
        long bitIndex = 0;
        for (byte b : buf) {
            for (int i = 0; i < 8; i++) {
                if ((b & 0x01) == 0x01)
                    asserted.add(bitIndex + i);
                b >>>= 1;
            }
            bitIndex += 8;
        }
        return Utils.unbox(asserted.toArray(new Long[asserted.size()]));
    }
    
    private static final Random rand = new Random(System.currentTimeMillis());
    
    public static byte[] randomBytes(int size) {
        byte[] buf = new byte[size];
        rand.nextBytes(buf);
        return buf;
    }
    
    public static Set<Long> hashSetFrom(long[] longs) {
        Set<Long> set = new HashSet<Long>();
        for (long l : longs)
            set.add(l);
        return set;
    }
}

