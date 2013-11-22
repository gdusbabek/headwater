package headwater.bitmap;

public class TestSegmentedJuBitmap extends TestSegmentedBitmap {
    @Override
    public void instantiateFactory() {
        factory = JuBitmap.FACTORY;
    }
}
