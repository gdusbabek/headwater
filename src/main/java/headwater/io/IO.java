package headwater.io;

import headwater.bitmap.IBitmap;

public interface IO {
    public void put(byte[] key, long col, IBitmap value) throws Exception;
    public IBitmap get(byte[] key, long col) throws Exception;
    public void del(byte[] key, long col) throws Exception;
    public void visitAllColumns(byte[] key, int pageSize, ColumnObserver observer) throws Exception;
}
