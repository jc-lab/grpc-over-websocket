package kr.jclab.grpcoverwebsocket.client;

import kr.jclab.grpcoverwebsocket.core.protocol.v1.HandshakeResult;

import java.nio.ByteBuffer;

public interface ClientListener<C extends GrpcOverWebsocketClientConnection> {
    default void onConnected(C connection) {}

    default void onClosed(C connection) {}

    default void onError(C connection, Exception ex) {}

    /**
     * beforeReconnect
     *
     * @return Returning false cancels the reconnection.
     */
    default boolean beforeReconnect(C connection) {
        return true;
    }

    default void onHandshakeMessage(C connection, ByteBuffer buffer) {}

    default void onHandshakeResult(C connection, HandshakeResult handshakeResult) {}
}
