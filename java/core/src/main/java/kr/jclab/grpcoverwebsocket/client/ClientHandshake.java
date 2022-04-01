package kr.jclab.grpcoverwebsocket.client;

import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;

public interface ClientHandshake {
    ScheduledExecutorService getScheduledExecutorService();
    void sendMessage(ByteBuffer byteBuffer);
}
