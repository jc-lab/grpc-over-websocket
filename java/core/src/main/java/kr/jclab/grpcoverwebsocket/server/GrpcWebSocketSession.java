package kr.jclab.grpcoverwebsocket.server;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public interface GrpcWebSocketSession {
    String getId();
    void sendMessage(ByteBuffer buffer) throws IOException;
    void close();
    String getAuthority();
    SocketAddress getRemoteAddress();
    SocketAddress getLocalAddress();
}
