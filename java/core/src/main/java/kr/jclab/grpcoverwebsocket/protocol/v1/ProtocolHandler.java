package kr.jclab.grpcoverwebsocket.protocol.v1;

import kr.jclab.grpcoverwebsocket.core.protocol.v1.*;
import kr.jclab.grpcoverwebsocket.internal.HandshakeState;

import java.nio.ByteBuffer;
import java.util.EnumSet;

public interface ProtocolHandler<TContext> {
    HandshakeState getHandshakeState();
    void handleHandshakeMessage(TContext context, ByteBuffer payload);
    void handleHandshakeResult(TContext context, HandshakeResult payload);
    void handleNewStream(TContext context, NewStream payload);
    void handleStreamHeader(TContext context, StreamHeader payload);
    void handleGrpcStream(TContext context, int streamId, EnumSet<GrpcStreamFlag> flags, ByteBuffer data);
    void handleCloseStream(TContext context, CloseStream payload);
    void handleFinishTransport(TContext context, FinishTransport payload);
}
