package kr.jclab.grpcoverwebsocket.client.internal;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.grpc.*;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.internal.ByteBufferReadableBuffer;
import kr.jclab.grpcoverwebsocket.internal.ByteBufferWritableBufferAllocator;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.grpc.internal.ClientStreamListener.RpcProgress.PROCESSED;

@Slf4j
public class ClientStreamImpl extends AbstractClientStream {
    static final int ABSENT_ID = -1;

    private final MethodDescriptor<?, ?> method;
    private final AbstractCgrpcClientTransport transport;
    private final TransportStateImpl state;
    private final Sink sink = new Sink();
    private final StatsTraceContext statsTraceCtx;

    public ClientStreamImpl(
            FrameWriter frameWriter,
            AbstractCgrpcClientTransport transport,
            Object lock,
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

        this.state = new TransportStateImpl(
                ByteBufferWritableBufferAllocator.MAX_BUFFER,
                statsTraceCtx,
                transportTracer,
                lock,
                frameWriter
        );
    }

    @Override
    public TransportStateImpl transportState() {
        return this.state;
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

    public void closeByForcelly(Status reason) {
        this.state.transportReportStatus(reason, true, new Metadata());
    }

    class Sink implements AbstractClientStream.Sink {
        @Override
        public void writeHeaders(Metadata metadata, @Nullable byte[] payload) {
            log.trace("writeHeaders");
            synchronized (state.lock) {
                state.streamReady(metadata, method.getFullMethodName(), payload);
            }
        }

        @Override
        public void writeFrame(@Nullable WritableBuffer frame, boolean endOfStream, boolean flush, int numMessages) {
            synchronized (state.lock) {
                state.sendBuffer(frame, endOfStream, flush);
                getTransportTracer().reportMessageSent(numMessages);
            }
        }

        @Override
        public void cancel(Status status) {
            state.cancel(status, true, null);
        }
    }

    class TransportStateImpl extends TransportState {
        private final FrameWriter frameWriter;
        private final Object lock;

        @GuardedBy("lock")
        private Metadata requestMetadata = null;

        private Status transportError = null;
        private Metadata transportErrorMetadata = null;
        private boolean headersReceived = false;

        @GuardedBy("lock") private boolean cancelSent = false;
        @GuardedBy("lock") private boolean canStart = true;

        private int id = ABSENT_ID;

        protected TransportStateImpl(
                int maxMessageSize,
                StatsTraceContext statsTraceCtx,
                TransportTracer transportTracer,
                Object lock,
                FrameWriter frameWriter
        ) {
            super(maxMessageSize, statsTraceCtx, transportTracer);
            this.lock = checkNotNull(lock, "lock");
            this.frameWriter = frameWriter;
        }

        @SuppressWarnings("GuardedBy")
        @GuardedBy("lock")
        public void start(int streamId, String fullMethodName, @Nullable byte[] payload) {
            checkState(id == ABSENT_ID, "the stream has been started with id %s", streamId);
            this.id = streamId;

            onStreamAllocated();

            if (canStart) {
                List<ByteString> serializedMetadata = Arrays.stream(InternalMetadata.serialize(requestMetadata))
                        .map(ByteString::copyFrom)
                        .collect(Collectors.toList());

                // Only happens when the stream has neither been started nor cancelled.
                frameWriter.newStream(streamId, serializedMetadata, fullMethodName, payload);
                statsTraceCtx.clientOutboundHeaders();
                requestMetadata = null;

//                if (pendingData.size() > 0) {
//                    outboundFlow.data(
//                            pendingDataHasEndOfStream, outboundFlowState, pendingData, flushPendingData);
//
//                }
                canStart = false;
            }
        }

        @GuardedBy("lock")
        @Override
        public void onStreamAllocated() {
            super.onStreamAllocated();
            getTransportTracer().reportLocalStreamStarted();
        }

        /**
         * Must be called with holding the transport lock.
         */
        @GuardedBy("lock")
        public void transportHeadersReceived(Metadata headers) {
            Preconditions.checkNotNull(headers, "headers");

            log.trace("transportHeadersReceived");

            try {
                if (headersReceived) {
                    transportError = Status.INTERNAL.withDescription("Received headers twice");
                    return;
                }
                headersReceived = true;

                stripTransportDetails(headers);
                inboundHeadersReceived(headers);
            } finally {
                if (transportError != null) {
                    // Note we don't immediately report the transport error, instead we wait for more data on
                    // the stream so we can accumulate more detail into the error before reporting it.
                    transportError = transportError.augmentDescription("headers: " + headers);
                    transportErrorMetadata = headers;
                }
            }
        }

        /**
         * Must be called with holding the transport lock.
         */
        @GuardedBy("lock")
        public void transportTrailersReceived(Metadata trailers, Status status) {
            Preconditions.checkNotNull(trailers, "trailers");
            if (transportError != null) {
                transportError = transportError.augmentDescription("trailers: " + trailers);
                // http2ProcessingFailed(transportError, false, transportErrorMetadata);
            } else {
                stripTransportDetails(trailers);
                inboundTrailersReceived(trailers, status);
            }
        }

        @Override
        @GuardedBy("lock")
        public void deframeFailed(Throwable cause) {
            cause.printStackTrace();
        }

        @Override
        @GuardedBy("lock")
        public void bytesRead(int numBytes) {
        }

        @Override
        @GuardedBy("lock")
        public void deframerClosed(boolean hasPartialMessage) {
            onEndOfStream();
            super.deframerClosed(hasPartialMessage);
        }

        @Override
        @GuardedBy("lock")
        public void runOnTransportThread(final Runnable r) {
            synchronized (lock) {
                r.run();
            }
        }

        /**
         * Must be called with holding the transport lock.
         */
        @GuardedBy("lock")
        public void transportDataReceived(ByteBuffer data, boolean endOfStream) {
            if (transportError != null) {
                // We've already detected a transport error and now we're just accumulating more detail
                // for it.
                // transportError = transportError.augmentDescription("DATA: ...");
                // frame.close();
                // if (transportError.getDescription().length() > 1000 || endOfStream) {
                //     http2ProcessingFailed(transportError, false, transportErrorMetadata);
                // }
            } else {
                if (!headersReceived) {
//                    http2ProcessingFailed(
//                            Status.INTERNAL.withDescription("headers not received before payload"),
//                            false,
//                            new Metadata());
                    return;
                }
                int frameSize = data.remaining();
                inboundDataReceived(
                        new ByteBufferReadableBuffer(data)
                );
                if (endOfStream) {
                    // This is a protocol violation as we expect to receive trailers.
                    if (frameSize > 0) {
                        transportError = Status.INTERNAL
                                .withDescription("Received unexpected EOS on non-empty DATA frame from server");
                    } else {
                        transportError = Status.INTERNAL
                                .withDescription("Received unexpected EOS on empty DATA frame from server");
                    }
                    transportErrorMetadata = new Metadata();
                    transportReportStatus(transportError, false, transportErrorMetadata);
                }
            }
        }

        @GuardedBy("lock")
        private void onEndOfStream() {
            if (!isOutboundClosed()) {
                // If server's end-of-stream is received before client sends end-of-stream, we just send a
                // reset to server to fully close the server side stream.
                transport.finishStream(id(),null, PROCESSED, false, Code.CANCELLED, null);
            } else {
                transport.finishStream(id(), null, PROCESSED, false, null, null);
            }
        }

        @SuppressWarnings("GuardedBy")
        @GuardedBy("lock")
        private void cancel(Status reason, boolean stopDelivery, Metadata trailers) {
            if (cancelSent) {
                return;
            }
            cancelSent = true;
            if (canStart) {
                // stream is pending.
                // TODO(b/145386688): This access should be guarded by 'this.transport.lock'; instead found:
                // 'this.lock'
//                transport.removePendingStream(OkHttpClientStream.this);
                // release holding data, so they can be GCed or returned to pool earlier.
//                requestHeaders = null;
//                pendingData.clear();
                canStart = false;
                transportReportStatus(reason, true, trailers != null ? trailers : new Metadata());
            } else {
                // If pendingData is null, start must have already been called, which means synStream has
                // been called as well.
                transport.finishStream(id(), reason, PROCESSED, stopDelivery, Code.CANCELLED, trailers);
            }
        }

        @SuppressWarnings("GuardedBy")
        @GuardedBy("lock")
        private void sendBuffer(WritableBuffer frame, boolean endOfStream, boolean flush) {
            if (cancelSent) {
                return;
            }
            if (canStart) {
                throw new RuntimeException("ILLEGAL STATE");
            } else {
                checkState(id() != ABSENT_ID, "streamId should be set");
                frameWriter.data(id(), frame, endOfStream);
            }
        }

        @SuppressWarnings("GuardedBy")
        @GuardedBy("lock")
        private void streamReady(Metadata metadata, String fullMethodName, @Nullable byte[] payload) {
            requestMetadata = metadata;
            transport.streamReadyToStart(ClientStreamImpl.this, fullMethodName, payload);
        }

        /**
         * Strip HTTP transport implementation details so they don't leak via metadata into
         * the application layer.
         */
        void stripTransportDetails(Metadata metadata) {
            metadata.discardAll(InternalStatus.CODE_KEY);
            metadata.discardAll(InternalStatus.MESSAGE_KEY);
        }

        int id() {
            return id;
        }
    }
}
