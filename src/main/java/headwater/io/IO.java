package headwater.io;

public interface IO<C,V> {
    public void put(byte[] key, C col, V value) throws Exception;
    public V get(byte[] key, C col) throws Exception;
    public void del(byte[] key, C col) throws Exception;
    public void visitAllColumns(byte[] key, int pageSize, ColumnObserver<C,V> observer) throws Exception;
}
