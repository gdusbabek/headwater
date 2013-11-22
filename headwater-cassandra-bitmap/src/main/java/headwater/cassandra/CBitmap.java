package headwater.cassandra;

import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import headwater.bitmap.AbstractBitmap;
import headwater.bitmap.IBitmap;
import headwater.bitmap.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CBitmap extends AbstractBitmap {
    
    private IO io;
    
    private final long numBits;
    private final int columnWidthInBits;
    private final byte[] key;
    
    public CBitmap(byte[] key, long numBits, int columnWidthInBits, IO io) {
        if (numBits % columnWidthInBits != 0)
            throw new IllegalArgumentException("numBits % columnWidthInBits != 0");
        if (columnWidthInBits % 8 != 0)
            throw new IllegalArgumentException("columnWidthInBits % 8 != 0");
        if (io == null)
            throw new IllegalArgumentException("IO cannot be null!");
        
        this.key = key;
        this.numBits = numBits;
        this.columnWidthInBits = columnWidthInBits;
        this.io = io;
    }

    public long getBitLength() {
        return numBits;
    }

    public void set(long bit, boolean value) {
        // which column is that one?
        long columnName = bit / columnWidthInBits;
        // which bit in the column. convert to bytes so we can read it.
        int modBit = (int)(bit % columnWidthInBits);
        byte[] col = Utils.longToBytes(columnName);
        try {
            // get the value and set/unset it.
            byte[] curValue;
            try {
                curValue = io.get(key, col);
            } catch (NotFoundException ex) {
                curValue = new byte[columnWidthInBits / 8];
            }
            if (value)
                flipOn(curValue, modBit);
            else
                flipOff(curValue, modBit);
            io.put(key, col, curValue);
        } catch (Exception ex) {
            // for now:
            ex.printStackTrace();
            throw new IndexOperationException(ex.getMessage(), ex);
        }
    }

    public void set(long... bits) {
        for (long bit : bits) {
            set(bit, true);
        }
    }

    public boolean get(long bit) {
        long columnName = bit / columnWidthInBits;
        int modBit = (int)(bit % columnWidthInBits);
        byte[] col = Utils.longToBytes(columnName);
        try {
            byte[] curValue;
            try {
                curValue = io.get(key, col);
            } catch (NotFoundException ex) {
                return false; // nothing was there.
            }
            int index = modBit / 8;
            int bitInByte = modBit % 8;
            return (curValue[index] & (0x01 << bitInByte)) > 0;
        } catch (Exception ex) {
            throw new IndexOperationException(ex.getMessage(), ex);
        }
    }

    public long[] getAsserted() {
        final List<Long> asserted = new ArrayList<Long>();
        try {
            io.visitAllColumns(key, 100, new ColumnObserver() {
                public void observe(byte[] row, byte[] col, byte[] value) {
                    long bitIndex = Utils.bytesToLong(col) * columnWidthInBits;
                    for (byte b : value) {
                        for (int i = 0; i < 8; i++) {
                            if ((b & 0x01) == 0x01)
                                asserted.add(bitIndex + i);
                            b >>>= 1;
                        }
                        bitIndex += 8;
                    }
                }
            });
        } catch (Exception ex) {
            throw new IndexOperationException(ex.getMessage(), ex);
        }
        return Utils.unbox(asserted.toArray(new Long[asserted.size()]));
    }

    public void clear() {
        final byte[] empty = new byte[columnWidthInBits / 8];
        try {
            io.visitAllColumns(key, 2048, new ColumnObserver() {
                public void observe(byte[] row, byte[] col, byte[] value) {
                    try {
                        io.put(key, col, empty);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        } catch (Exception ex) {
            throw new IndexOperationException(ex.getMessage(), ex);
        }
    }

    public boolean isEmpty() {
        final AtomicBoolean empty = new AtomicBoolean(true);
        try {
            io.visitAllColumns(key, 2048, new ColumnObserver() {
                public void observe(byte[] row, byte[] col, byte[] value) {
                    if (!empty.get()) return;
                    for (byte b : value) {
                        if (b != 0) {
                            empty.set(false);
                        }
                    }
                }
            });
        } catch (Exception ex) {
            throw new IndexOperationException(ex.getMessage(), ex);
        }
        return empty.get();
    }

    // todo: shouldn't need this, or should be unsafe.
    public byte[] toBytes() {
        final AtomicInteger size = new AtomicInteger(0);
        final List<byte[]> buffers = new ArrayList<byte[]>();
        try {
            io.visitAllColumns(key, 2048, new ColumnObserver() {
                public void observe(byte[] row, byte[] col, byte[] value) {
                    size.addAndGet(value.length);
                    buffers.add(value);
                }
            });
        } catch (Exception ex) {
            throw new IndexOperationException(ex.getMessage(), ex);
        }
        
        byte[] notPadded = new byte[size.get()];
        int pos = 0;
        for (byte[] buf : buffers) {
            System.arraycopy(buf, 0, notPadded, pos, buf.length);
            pos += buf.length;
        }
        
        if (notPadded.length < numBits / 8) {
            byte[] all = new byte[(int)numBits / 8];
            System.arraycopy(notPadded, 0, all, 0, notPadded.length);
            return all;
        } else {
            return notPadded;
        }
    }

    // todo: ouch! fix this.
    public byte[] toBytes(int byteStart, int numBytes) {
        byte[] buf = toBytes();
        byte[] newBuf = new byte[numBytes];
        System.arraycopy(buf, byteStart, newBuf, 0, numBytes);
        return newBuf;
    }

    @Override
    public Object clone() {
        final byte[] newKey = Utils.randomBytes(128);
        try {
            io.visitAllColumns(key, 2048, new ColumnObserver() {
                public void observe(byte[] row, byte[] col, byte[] value) {
                    try {
                        io.put(newKey, col, value);
                    } catch (Exception ex) {
                        // ugh.
                        throw new RuntimeException(ex);
                    }
                }
            });
        } catch (Exception ex) {
            throw new RuntimeException("Error cloning bitmap", ex);
        }
        return new CBitmap(newKey, numBits, columnWidthInBits, io);
    }

    //
    //
    //
    
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
}
