package headwater.hashing;

public interface BitHashableKey<K>{
    public long getHashBit();
    public K getKey();
    public byte[] asBytes();
}
