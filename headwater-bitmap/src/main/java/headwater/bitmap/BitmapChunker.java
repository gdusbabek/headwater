package headwater.bitmap;

import java.util.ArrayList;
import java.util.List;

public class BitmapChunker {
    
    /**
     * return chunks. least significant chunks first. least significant bytes are first within the chunks.
     * @param bm
     * @param chunkArraySize    
     * @param skipEmpties
     * @return
     */
    public static List<Chunk> divide(IBitmap bm, int chunkArraySize, boolean skipEmpties) {
        final int numChunks = (int)((bm.getBitLength() / (chunkArraySize *8)) + ((bm.getBitLength() % (chunkArraySize * 8)) > 0 ? 1 : 0));
        List<Chunk> chunks = new ArrayList<Chunk>(numChunks);
        
        for (int i = 0; i < numChunks; i++) {
            byte[] chunk = bm.toBytes(i * chunkArraySize, chunkArraySize);
            if (skipEmpties) {
                for (byte ch : chunk) {
                    if (ch != 0) {
                        chunks.add(new Chunk(i, chunk));
                        break;
                    }
                }
            } else {
                chunks.add(new Chunk(i, chunk));
            }
        }
        return chunks;
    }
}
