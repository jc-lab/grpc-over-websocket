package kr.jclab.grpcoverwebsocket.server;

import java.nio.ByteBuffer;

public interface GrpcOverWebsocketServerHandler {
    default void onStart(GrpcOverWebsocketServer server) {}

    default void onTerminated(GrpcOverWebsocketServer server) {}

    default void onConnected(HandshakeContext handshakeContext) {
        handshakeContext.ready();
    }

    default void onClosed(GrpcWebSocketSession session) {}

    default void onError(GrpcWebSocketSession session, Exception ex) {}

    default void onHandshakeMessage(HandshakeContext handshakeContext, ByteBuffer buffer) {}
}
