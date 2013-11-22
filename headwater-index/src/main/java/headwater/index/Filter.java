package headwater.index;

public interface Filter<T> {
    public boolean matches(T t);
}
