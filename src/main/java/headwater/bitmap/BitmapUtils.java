package headwater.bitmap;

// todo: if IBitmap supports simple bitwise operations, use those.
public class BitmapUtils {
    public static void mutatingAnd(IBitmap receiver, IBitmap giver) {
        if (giver.getBitLength() != receiver.getBitLength()) throw new IllegalArgumentException("Bitset lengths are not identical");
        for (long i : receiver.getAsserted())
            receiver.set(i, giver.get(i));
    }

    public static IBitmap nonMutatingAND(IBitmap a, IBitmap b) {
        try {
            IBitmap likeA = (IBitmap)a.clone();
            BitmapUtils.mutatingAnd(likeA, b);
            return likeA;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void mutatingOR(IBitmap receiver, IBitmap giver) {
        if (giver.getBitLength() != receiver.getBitLength()) throw new IllegalArgumentException("Bitset lengths are not identical");
        for (long i : giver.getAsserted())
            receiver.set(i, true);
    }

    public static IBitmap nonMutatingOR(IBitmap a, IBitmap b) {
        try {
            IBitmap likeA = (IBitmap)a.clone();
            BitmapUtils.mutatingOR(likeA, b);
            return likeA;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
    }
}
