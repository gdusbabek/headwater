package headwater.io;

public interface IO<T> {
    public void put(byte[] key, long col, T value) throws Exception;
    public T get(byte[] key, long col) throws Exception;
    public void del(byte[] key, long col) throws Exception;
    public void visitAllColumns(byte[] key, int pageSize, ColumnObserver<T> observer) throws Exception;
}
