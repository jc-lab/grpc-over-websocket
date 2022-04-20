package kr.jclab.grpcoverwebsocket.client;

import kr.jclab.grpcoverwebsocket.core.protocol.v1.HandshakeResult;
import org.java_websocket.client.WebSocketClient;

import java.nio.ByteBuffer;

public interface ClientListener {
    default void onWebsocketCreated(GrpcOverWebsocketClientConnection connection, WebSocketClient webSocketClient) {}

    default void onConnected(GrpcOverWebsocketClientConnection connection) {}

    default void onClosed(GrpcOverWebsocketClientConnection connection) {}

    default void onError(GrpcOverWebsocketClientConnection connection, Exception ex) {}

    /**
     * beforeReconnect
     *
     * @return Returning false cancels the reconnection.
     */
    default boolean beforeReconnect(GrpcOverWebsocketClientConnection connection) {
        return true;
    }

    default void onHandshakeMessage(GrpcOverWebsocketClientConnection connection, ByteBuffer buffer) {}

    default void onHandshakeResult(GrpcOverWebsocketClientConnection connection, HandshakeResult handshakeResult) {}
}
