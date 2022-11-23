package kr.jclab.grpcoverwebsocket.portable;

import java.nio.ByteBuffer;

public interface ReadableSocket {
    void onMessage(ByteBuffer byteBuffer);
    void onPing();
    void onPong();
}
