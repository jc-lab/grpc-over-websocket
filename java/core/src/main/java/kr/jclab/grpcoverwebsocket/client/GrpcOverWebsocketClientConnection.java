package kr.jclab.grpcoverwebsocket.client;

import java.nio.ByteBuffer;

public interface GrpcOverWebsocketClientConnection {
    void sendHandshakeMessage(ByteBuffer byteBuffer);
    void goAway();
}
