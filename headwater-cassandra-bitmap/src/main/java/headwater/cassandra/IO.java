package headwater.cassandra;

public interface IO {
    public void put(byte[] key, byte[] col, byte[] value) throws Exception;
    public byte[] get(byte[] key, byte[] col) throws Exception;
    public void visitAllColumns(byte[] key, int pageSize, ColumnObserver observer) throws Exception;
}
