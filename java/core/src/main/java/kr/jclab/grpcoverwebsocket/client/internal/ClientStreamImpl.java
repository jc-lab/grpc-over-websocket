package kr.jclab.grpcoverwebsocket.client.internal;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.client.GrpcOverWebsocketClientTransport;
import kr.jclab.grpcoverwebsocket.core.protocol.v1.CloseStream;
import kr.jclab.grpcoverwebsocket.core.protocol.v1.NewStream;
import kr.jclab.grpcoverwebsocket.internal.ByteBufferReadableBuffer;
import kr.jclab.grpcoverwebsocket.internal.ByteBufferWritableBufferAllocator;
import kr.jclab.grpcoverwebsocket.protocol.v1.ControlType;
import kr.jclab.grpcoverwebsocket.protocol.v1.GrpcStreamFlag;
import kr.jclab.grpcoverwebsocket.protocol.v1.ProtocolHelper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class ClientStreamImpl extends AbstractClientStream {
    @Getter
    private int streamId = -1;

    private final MethodDescriptor<?, ?> method;
    private final GrpcOverWebsocketClientTransport transport;
    private final TransportStateImpl transportState;
    private final StatsTraceContext statsTraceCtx;

    public ClientStreamImpl(
            GrpcOverWebsocketClientTransport transport,
            WritableBufferAllocator bufferAllocator,
            StatsTraceContext statsTraceCtx,
            TransportTracer transportTracer,
            MethodDescriptor<?, ?> method,
            Metadata headers,
            CallOptions callOptions
    ) {
        super(bufferAllocator, statsTraceCtx, transportTracer, headers, callOptions, false);
        this.transport = transport;
        this.method = method;
        this.statsTraceCtx = statsTraceCtx;

        this.transportState = new TransportStateImpl(
                ByteBufferWritableBufferAllocator.MAX_BUFFER,
                statsTraceCtx,
                transportTracer
        );
    }

    @Override
    protected TransportState transportState() {
        return this.transportState;
    }

    @Override
    protected Sink abstractClientStreamSink() {
        return this.sink;
    }

    @Override
    public void setAuthority(String authority) {

    }

    @Override
    public Attributes getAttributes() {
        return null;
    }

    public void start(int streamId) {
        this.streamId = streamId;
        synchronized (this.transportState.lock) {
            this.transportState.onStreamAllocated();
        }
    }

    public void handleStreamHeader(Metadata metadata) {
        this.transportState.inboundHeadersReceived(
                metadata
        );
    }

    public void handleGrpcStream(EnumSet<GrpcStreamFlag> flags, ByteBuffer data) {
        this.transportState.inboundDataReceived(
                new ByteBufferReadableBuffer(data)
        );
    }

    public void handleCloseStream(Status status, @Nullable Metadata trailers) {
        this.transportState.inboundTrailersReceived(
                trailers,
                status
        );
    }

    private class TransportStateImpl extends TransportState {
        private final Object lock = new Object();
        @GuardedBy("lock") private boolean cancelSent = false;

        protected TransportStateImpl(int maxMessageSize, StatsTraceContext statsTraceCtx, TransportTracer transportTracer) {
            super(maxMessageSize, statsTraceCtx, transportTracer);
        }

        @Override
        public void runOnTransportThread(Runnable r) {
            r.run();
        }

        @Override
        public void bytesRead(int numBytes) {
        }

        @Override
        public void deframeFailed(Throwable cause) {

        }

        @Override
        public void inboundHeadersReceived(Metadata headers) {
            super.inboundHeadersReceived(headers);
        }

        @Override
        public void inboundDataReceived(ReadableBuffer frame) {
            super.inboundDataReceived(frame);
        }

        @Override
        public void inboundTrailersReceived(Metadata trailers, Status status) {
            super.inboundTrailersReceived(trailers, status);
        }

        @GuardedBy("lock")
        @Override
        public void onStreamAllocated() {
            super.onStreamAllocated();
            getTransportTracer().reportLocalStreamStarted();
        }

        // @GuardedBy("lock")
        void cancel(Status reason, boolean stopDelivery, Metadata trailers) {
            synchronized (this.lock) {
                if (this.cancelSent) {
                    return ;
                }
                this.cancelSent = true;

                CloseStream streamTrailers = CloseStream.newBuilder()
                        .setStreamId(streamId)
                        .setStatus(
                                com.google.rpc.Status.newBuilder()
                                        .setCode(
                                                reason.getCode().value()
                                        )
                                        .setMessage(
                                                Optional.ofNullable(reason.getDescription())
                                                        .orElse("")
                                        )
                                        .build()
                        )
                        .addAllTrailers(
                                ProtocolHelper.metadataSerialize(trailers)
                        )
                        .build();
                transport.sendControlMessage(ControlType.CloseStream, streamTrailers);

                if (trailers == null) {
                    trailers = new Metadata();
                }
                transportReportStatus(reason, stopDelivery, trailers);
            }
        }
    }

    private final Sink sink = new Sink() {
        @Override
        public void writeHeaders(Metadata metadata, @Nullable byte[] payload) {
            log.info("writeHeaders");
            transport.startStream(ClientStreamImpl.this);

            List<ByteString> serializedMetadata = Arrays.stream(InternalMetadata.serialize(metadata))
                    .map(ByteString::copyFrom)
                    .collect(Collectors.toList());

            NewStream.Builder builder = NewStream.newBuilder()
                    .setStreamId(streamId)
                    .addAllMetadata(serializedMetadata)
                    .setMethodName(method.getFullMethodName());
            if (payload != null) {
                builder.setPayload(ByteString.copyFrom(payload));
            }

            transport.getTransportQueue().enqueue(() -> {
                transport.sendControlMessage(ControlType.NewStream, builder.build());
                statsTraceCtx.clientOutboundHeaders();
            }, true);
        }

        @Override
        public void writeFrame(@Nullable WritableBuffer frame, boolean endOfStream, boolean flush, int numMessages) {
            transport.getTransportQueue().enqueue(() -> {
                transport.sendGrpcPayload(streamId, frame, endOfStream);
            }, true);
        }

        @Override
        public void cancel(Status status) {
            transportState.cancel(status, true, null);
        }
    };
}
