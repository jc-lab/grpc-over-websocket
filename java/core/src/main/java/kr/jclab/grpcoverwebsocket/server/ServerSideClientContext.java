package kr.jclab.grpcoverwebsocket.server;

public interface ServerSideClientContext {
    GrpcWebSocketSession getSession();
    void close();
}
