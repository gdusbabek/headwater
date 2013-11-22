package headwater.bitmap;

public class TestSegmentedOpenBitmap extends TestSegmentedBitmap {
    
    @Override
    public void instantiateFactory() {
        factory = OpenBitmap.FACTORY;
    }
}
