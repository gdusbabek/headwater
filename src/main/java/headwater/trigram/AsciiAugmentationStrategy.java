package headwater.trigram;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class AsciiAugmentationStrategy implements AugmentationStrategy {
    private static final int MIN_LETTER = (int)'a';
    private static final int MAX_LETTER = (int)'z';
    
    public Collection<Trigram> augment(final String parcel) {
        Set<Trigram> results = new HashSet<Trigram>();
        for (String pad : padding(3 - parcel.length())) {
            for (Trigram trigram : Trigram.make(pad + parcel))
                results.add(trigram);
            for (Trigram trigram : Trigram.make(parcel + pad))
                results.add(trigram);
        }
        return results;
    }
    
    public static Iterable<String> padding(final int size) {
        return new Iterable<String>() {
            final int[] buf = new int[size]; 
            {
                for (int i= 0; i < size; i++)
                    buf[i] = MIN_LETTER - 1;
            }
            
            
            public Iterator<String> iterator() {
                
                return new Iterator<String>() {
                    
                    public boolean hasNext() {
                        for (int i = 0; i < buf.length; i++) {
                            if (buf[i] != MAX_LETTER)
                                return true;
                        }
                        return false;
                    }

                    public String next() {
                        increment(size - 1);
                        StringBuilder sb = new StringBuilder();
                        for (int i : buf)
                            if (i >= MIN_LETTER)
                                sb = sb.append((char)i);
                        return sb.toString();
                    }
                    
                    private boolean increment(int pos) {
                        if (pos < 0)
                            return true;
                        buf[pos] += 1;
                        if (buf[pos] > MAX_LETTER) {
                            if (increment(pos - 1)) {
                                buf[pos] = MAX_LETTER;
                                return true;
                            }
                            buf[pos] = MIN_LETTER;
                        }
                        return false;
                    }

                    public void remove() {
                        throw new IllegalStateException("Not implemented");
                    }
                };
            }
        };
    }
}

