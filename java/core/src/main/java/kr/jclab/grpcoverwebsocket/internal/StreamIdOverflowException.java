package kr.jclab.grpcoverwebsocket.internal;

public class StreamIdOverflowException extends Exception {
    public StreamIdOverflowException() {
    }

    public StreamIdOverflowException(String message) {
        super(message);
    }

    public StreamIdOverflowException(String message, Throwable cause) {
        super(message, cause);
    }

    public StreamIdOverflowException(Throwable cause) {
        super(cause);
    }

    public StreamIdOverflowException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
