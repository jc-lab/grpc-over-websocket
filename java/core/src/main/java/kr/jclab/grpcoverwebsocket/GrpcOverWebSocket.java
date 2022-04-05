package kr.jclab.grpcoverwebsocket;

import io.grpc.Attributes;
import io.grpc.Grpc;
import kr.jclab.grpcoverwebsocket.server.GrpcWebSocketSession;

public class GrpcOverWebSocket {
    private GrpcOverWebSocket() {}

    @Grpc.TransportAttr
    public static final Attributes.Key<GrpcWebSocketSession> SESSION =
            Attributes.Key.create("gow-session");
}
