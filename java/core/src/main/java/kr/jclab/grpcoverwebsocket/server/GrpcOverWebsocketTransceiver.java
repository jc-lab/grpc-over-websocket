package kr.jclab.grpcoverwebsocket.server;

import java.nio.ByteBuffer;

public class GrpcOverWebsocketTransceiver implements WebsocketHandler {
    private WebsocketHandler delegate;

    public void setDelegate(WebsocketHandler grpcWebSocketServerHandler) {
        this.delegate = grpcWebSocketServerHandler;
    }

    @Override
    public void afterConnectionEstablished(GrpcWebSocketSession session) throws Exception {
        this.delegate.afterConnectionEstablished(session);
    }

    @Override
    public void handleMessage(GrpcWebSocketSession session, ByteBuffer message) throws Exception {
        this.delegate.handleMessage(session, message);
    }

    @Override
    public void afterConnectionClosed(GrpcWebSocketSession session) throws Exception {
        this.delegate.afterConnectionClosed(session);
    }
}
