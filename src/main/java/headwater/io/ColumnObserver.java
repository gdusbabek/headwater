package headwater.io;

import headwater.bitmap.IBitmap;

public interface ColumnObserver {
    public void observe(byte[] row, long col, IBitmap value);
}
