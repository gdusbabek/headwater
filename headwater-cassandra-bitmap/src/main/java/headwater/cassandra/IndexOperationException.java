package headwater.cassandra;

public class IndexOperationException extends RuntimeException {
    public IndexOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
