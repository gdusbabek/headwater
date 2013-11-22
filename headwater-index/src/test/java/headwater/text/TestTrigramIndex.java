package headwater.text;

import headwater.bitmap.BitmapFactory;
import headwater.bitmap.IBitmap;
import headwater.bitmap.OpenBitmap;
import headwater.data.DataAccess;
import headwater.data.MemoryDataAccess;

public class TestTrigramIndex extends AbstractTrigramIndexTest {
    
    @Override
    public DataAccess<String, String, String> makeDataAccess() {
        return new MemoryDataAccess<String, String, String>();
    }

    @Override
    public BitmapFactory makeBitmapFactory(final int bits) {
        return new BitmapFactory() {
            public IBitmap newBitmap(int numBits) {
                throw new RuntimeException("Not implemented");
            }
    
            public IBitmap newBitmap() {
                return new OpenBitmap(bits);
            }
        };
    }
}
