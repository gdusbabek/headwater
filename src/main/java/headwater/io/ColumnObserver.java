package headwater.io;

import headwater.bitmap.IBitmap;

public interface ColumnObserver<T> {
    public void observe(byte[] row, long col, T value);
}
