package headwater.data;

public interface ColumnObserver {
    public void observe(byte[] row, byte[] col, byte[] value);
}
