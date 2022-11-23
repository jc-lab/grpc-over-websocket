package kr.jclab.grpcoverwebsocket.portable;

import java.nio.ByteBuffer;

public interface WritableSocket {
    void connect();
    void send(ByteBuffer byteBuffer);
    void sendPing();
    void close();
}
