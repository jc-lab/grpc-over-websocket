package kr.jclab.grpcoverwebsocket.server;

import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;

public interface HandshakeContext {
    ScheduledExecutorService getScheduledExecutorService();
    GrpcWebSocketSession getSession();
    void ready(byte[] metadata);
    void reject(String message, byte[] metadata);
    void sendHandshakeMessage(ByteBuffer buffer);

    default void ready() {
        this.ready(null);
    }
    default void reject(String message) {
        this.reject(message, null);
    }
}
