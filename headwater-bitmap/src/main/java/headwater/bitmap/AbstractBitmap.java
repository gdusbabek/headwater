package headwater.bitmap;

/**
 * relies on getBitLength(), getAsserted(), get(), clone()
 */
public abstract class AbstractBitmap implements IBitmap {
    
    public abstract Object clone();
    
    public IBitmap mand(IBitmap other) {
        if (other.getBitLength() != this.getBitLength()) throw new IllegalArgumentException("Bitset lengths are not identical");
        for (long i : getAsserted())
            set(i, other.get(i));
        return this;
    }

    public IBitmap and(IBitmap other) {
        IBitmap likeThis = (IBitmap)this.clone();
        return likeThis.mand(other);
    }

    public IBitmap mor(IBitmap other) {
        if (other.getBitLength() != this.getBitLength()) throw new IllegalArgumentException("Bitset lengths are not identical");
        for (long i : other.getAsserted())
            set(i, true);
        return this;
    }

    public IBitmap or(IBitmap other) {
        IBitmap likeThis = (IBitmap)this.clone();
        return likeThis.mor(other);
    }
}
