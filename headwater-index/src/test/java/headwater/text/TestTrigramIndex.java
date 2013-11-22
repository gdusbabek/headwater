package headwater.text;

import headwater.bitmap.BitmapFactory;
import headwater.bitmap.IBitmap;
import headwater.bitmap.OpenBitmap;
import headwater.data.KeyObserver;
import headwater.data.MemoryKeyObserver;
import headwater.hash.FunnelHasher;
import headwater.hash.Hashers;

public class TestTrigramIndex extends AbstractTrigramIndexTest {

    @Override
    public ITrigramIndex<String, String> makeIndex() {
        final int bits = 0x00100000;
        
        KeyObserver<String, String, String> observer = new MemoryKeyObserver<String, String, String>();
        
        BitmapFactory bitmapFactory = new BitmapFactory() {
            public IBitmap newBitmap(int numBits) {
                throw new RuntimeException("Not implemented");
            }
    
            public IBitmap newBitmap() {
                return new OpenBitmap(bits);
            }
        };
        
        FunnelHasher<String> keyHasher = Hashers.makeHasher(String.class, 128);
        return new TrigramIndex<String, String>(
                keyHasher, 
                Hashers.makeHasher(String.class, 128), 
                128)
                .withBitmapFactory(bitmapFactory)
                .withIndexObserver(observer);
    }
}
