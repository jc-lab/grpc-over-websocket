package kr.jclab.grpcoverwebsocket.client;

import org.java_websocket.client.WebSocketClient;

public interface WebsocketClientListener<C extends GrpcOverWebsocketClientConnection> extends ClientListener<C> {
    default void onWebsocketCreated(C connection, WebSocketClient webSocketClient) {}
}
