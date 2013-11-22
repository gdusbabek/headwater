package headwater.cassandra;

public interface ColumnObserver {
    public void observe(byte[] row, byte[] col, byte[] value);
}
