package kr.jclab.grpcoverwebsocket.server;

import java.nio.ByteBuffer;

public interface WebsocketHandler {
    /**
     * Invoked after WebSocket negotiation has succeeded and the WebSocket connection is
     * opened and ready for use.
     * @throws Exception this method can handle or propagate exceptions; see class-level
     * Javadoc for details.
     */
    void afterConnectionEstablished(GrpcWebSocketSession session) throws Exception;

    /**
     * Invoked when a new WebSocket message arrives.
     * @throws Exception this method can handle or propagate exceptions; see class-level
     * Javadoc for details.
     */
    void handleMessage(GrpcWebSocketSession session, ByteBuffer message) throws Exception;

    /**
     * Invoked after the WebSocket connection has been closed by either side, or after a
     * transport error has occurred. Although the session may technically still be open,
     * depending on the underlying implementation, sending messages at this point is
     * discouraged and most likely will not succeed.
     * @throws Exception this method can handle or propagate exceptions; see class-level
     * Javadoc for details.
     */
    void afterConnectionClosed(GrpcWebSocketSession session) throws Exception;
}
