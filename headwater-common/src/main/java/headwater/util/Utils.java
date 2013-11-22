package headwater.util;

import java.util.Random;

public class Utils {
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
    
    private static final Random rand = new Random(System.currentTimeMillis());
    
    public static byte[] randomBytes(int size) {
        byte[] buf = new byte[size];
        rand.nextBytes(buf);
        return buf;
    }
    
    public static void flipOn(byte[] buf, int bit) {
        int index = bit / 8;
        int bitInByte = bit % 8;
        buf[index] |= 0x01 << bitInByte;
    }
    
    public static void flipOff(byte[] buf, int bit) {
        int index = bit / 8;
        int bitInByte = bit % 8;
        buf[index] &= ~(0x01 << bitInByte);
    }
}
