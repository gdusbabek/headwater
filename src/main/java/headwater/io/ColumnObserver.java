package headwater.io;


public interface ColumnObserver<C,V> {
    public void observe(byte[] row, C col, V value);
}
