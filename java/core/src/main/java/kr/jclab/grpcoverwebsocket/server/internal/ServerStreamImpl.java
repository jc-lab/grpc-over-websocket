package kr.jclab.grpcoverwebsocket.server.internal;

import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.core.protocol.v1.CloseStream;
import kr.jclab.grpcoverwebsocket.core.protocol.v1.StreamHeader;
import kr.jclab.grpcoverwebsocket.internal.ByteBufferReadableBuffer;
import kr.jclab.grpcoverwebsocket.internal.ByteBufferWritableBufferAllocator;
import kr.jclab.grpcoverwebsocket.protocol.v1.ControlType;
import kr.jclab.grpcoverwebsocket.protocol.v1.GrpcStreamFlag;
import kr.jclab.grpcoverwebsocket.protocol.v1.ProtocolHelper;
import kr.jclab.grpcoverwebsocket.server.command.CancelServerStreamCommand;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Optional;

@Slf4j
public class ServerStreamImpl extends AbstractServerStream {
    private final String authority;
    private final Attributes attributes;

    private final int streamId;
    private final ServerTransportImpl transport;
    private final TransportStateImpl transportState;

    private boolean cancelSent = false;

    public ServerStreamImpl(
            WritableBufferAllocator bufferAllocator,
            String authority,
            Attributes attributes,
            StatsTraceContext statsTraceCtx,
            TransportTracer transportTracer,
            ServerTransportImpl transport,
            int streamId
    ) {
        super(bufferAllocator, statsTraceCtx);
        this.authority = authority;
        this.attributes = attributes;
        this.transport = transport;
        this.streamId = streamId;

        this.transportState = new TransportStateImpl(
                ByteBufferWritableBufferAllocator.MAX_BUFFER,
                statsTraceCtx,
                transportTracer
        );
    }

    public void start() {
        this.transportState.onStreamAllocated();
    }

    public int getStreamId() {
        return this.streamId;
    }

    public void handlePayload(EnumSet<GrpcStreamFlag> flags, ByteBuffer data) {
        ByteBufferReadableBuffer readableBuffer = new ByteBufferReadableBuffer(data);
        this.transportState.inboundDataReceived(readableBuffer, flags.contains(GrpcStreamFlag.EndOfFrame));
    }

    @Override
    protected TransportState transportState() {
        return this.transportState;
    }

    @Override
    protected Sink abstractServerStreamSink() {
        return this.sink;
    }

    @Override
    public int streamId() {
        return this.streamId;
    }

    @Override
    public String getAuthority() {
        return this.authority;
    }

    @Override
    public Attributes getAttributes() {
        return this.attributes;
    }

    public void cancelStream(Status reason, boolean remote) {
        if (this.cancelSent) {
            return ;
        }
        this.cancelSent = true;

        transportState.transportReportStatus(reason);
        if (!remote) {
            CloseStream streamTrailers = CloseStream.newBuilder()
                    .setStreamId(streamId)
                    .setStatus(ProtocolHelper.statusToProto(reason))
                    .build();
            transport.getTransportQueue().enqueue(() -> {
                transport.sendControlMessage(ControlType.CloseStream, streamTrailers);
            }, true);
        }
    }

    private void closeStreamWhenDone() {
        transportState.complete();
    }

    public class TransportStateImpl extends TransportState {
        protected TransportStateImpl(int maxMessageSize, StatsTraceContext statsTraceCtx, TransportTracer transportTracer) {
            super(maxMessageSize, statsTraceCtx, transportTracer);
        }

        @Override
        public void runOnTransportThread(Runnable r) {
            transport.getTransportQueue().enqueue(r, true);
        }

        @Override
        public void bytesRead(int numBytes) {

        }

        @Override
        public void deframeFailed(Throwable cause) {
            Status status = Status.fromThrowable(cause);
            transportReportStatus(status);
            transport.getTransportQueue().enqueue(new CancelServerStreamCommand(ServerStreamImpl.this, status), true);
        }
    }

    private final Sink sink = new Sink() {
        @Override
        public void writeHeaders(Metadata headers) {
            StreamHeader streamHeader = StreamHeader.newBuilder()
                    .setStreamId(streamId)
                    .addAllHeaders(ProtocolHelper.metadataSerialize(headers))
                    .build();
            transport.getTransportQueue()
                    .enqueue(() -> {
                        transport.sendControlMessage(ControlType.StreamHeader, streamHeader);
                    }, true);
        }

        @Override
        public void writeFrame(@Nullable WritableBuffer frame, boolean flush, int numMessages) {
            transport.getTransportQueue()
                    .enqueue(() -> {
                        transport.sendGrpcPayload(streamId, frame);
                    }, true);
        }

        @Override
        public void writeTrailers(Metadata trailers, boolean headersSent, Status status) {
            CloseStream streamTrailers = CloseStream.newBuilder()
                    .setStreamId(streamId)
                    .setStatus(
                            com.google.rpc.Status.newBuilder()
                                    .setCode(
                                            status.getCode().value()
                                    )
                                    .setMessage(
                                            Optional.ofNullable(status.getDescription())
                                                    .orElse("")
                                    )
                                    .build()
                    )
                    .addAllTrailers(ProtocolHelper.metadataSerialize(trailers))
                    .build();
            transport.getTransportQueue()
                    .enqueue(() -> {
                        transport.sendControlMessage(ControlType.CloseStream, streamTrailers);
                        closeStreamWhenDone();
                    }, true);
        }

        @Override
        public void cancel(Status status) {
            transport.getTransportQueue().enqueue(new CancelServerStreamCommand(ServerStreamImpl.this, status), true);
        }
    };
}
