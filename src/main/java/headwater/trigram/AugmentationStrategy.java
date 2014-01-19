package headwater.trigram;

import java.util.Collection;

public interface AugmentationStrategy {
    public Collection<Trigram> augment(String parcel);
}
