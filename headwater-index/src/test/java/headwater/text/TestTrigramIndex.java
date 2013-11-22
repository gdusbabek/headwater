package headwater.text;

import headwater.bitmap.BitmapFactory;
import headwater.bitmap.IBitmap;
import headwater.bitmap.OpenBitmap;
import headwater.data.MemLookupObserver;
import headwater.hash.FunnelHasher;
import headwater.hash.Hashers;

public class TestTrigramIndex extends AbstractTrigramIndexTest {

    @Override
    public ITrigramIndex<String, String> makeIndex() {
        final int bits = 0x00100000;
        
        MemLookupObserver<String, String, String> dataAccess = new MemLookupObserver<String, String, String>();
        
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
                .withKeyObserver(dataAccess)
                .withObjectLookup(dataAccess);
    }
}
