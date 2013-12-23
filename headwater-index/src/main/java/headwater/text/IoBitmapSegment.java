package headwater.text;

import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import headwater.bitmap.AbstractBitmap;
import headwater.data.IO;
import headwater.util.Utils;

import java.io.IOError;
import java.util.ArrayList;
import java.util.List;

public class IOBitmapSegment extends AbstractBitmap {
        
    protected byte[] rowKey;
    protected byte[] colName;
    
    private final int bitLength;
    private IO io;
    
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
