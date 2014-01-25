package headwater.io;

public interface BatchIO<C,V> extends IO<C,V> {
    public void begin();
    public void commit();
}
