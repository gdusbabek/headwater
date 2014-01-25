package headwater.io;

import headwater.bitmap.IBitmap;

public interface ColumnObserver<C,V> {
    public void observe(byte[] row, C col, V value);
}
