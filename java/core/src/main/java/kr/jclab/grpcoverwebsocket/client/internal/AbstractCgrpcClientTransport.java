package kr.jclab.grpcoverwebsocket.client.internal;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.rpc.Code;
import io.grpc.*;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.client.ClientListener;
import kr.jclab.grpcoverwebsocket.client.GrpcOverWebsocketClientConnection;
import kr.jclab.grpcoverwebsocket.core.protocol.v1.*;
import kr.jclab.grpcoverwebsocket.internal.*;
import kr.jclab.grpcoverwebsocket.portable.ClientSocket;
import kr.jclab.grpcoverwebsocket.portable.WritableSocket;
import kr.jclab.grpcoverwebsocket.protocol.v1.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract Customizable GRPC Client Transport
 */
@Slf4j
public class AbstractCgrpcClientTransport<C extends AbstractCgrpcClientTransport<C>> implements
        ConnectionClientTransport,
        GrpcOverWebsocketClientConnection,
        ClientSocket
{
    private final ScheduledExecutorService scheduledExecutorService;

    private final int maxInboundMetadataSize;
    private final int maxInboundMessageSize;

    private boolean enableKeepAlive = false;
    private long keepAliveTimeNanos = -1;
    private long keepAliveTimeoutNanos = -1;
    private boolean keepAliveWithoutCalls = false;

    private KeepAliveManager keepAliveManager = null;

    @Getter
    private final ClientTransportFactory.ClientTransportOptions options;

    private final OrderedQueue transportQueue;

    private final ClientListener<C> clientListener;
    private final InternalLogId logId;

    private final StreamIdGenerator streamIdGenerator = new StreamIdGenerator(StreamIdGenerator.Mode.Client);
    private final WritableBufferAllocator writableBufferAllocator = new ByteBufferWritableBufferAllocator();
    private final Attributes attributes;

    private final Object lock = new Object();
    @GuardedBy("lock") private HandshakeState handshakeState = HandshakeState.HANDSHAKE;
    @GuardedBy("lock") private Status goAwayStatus = null;
    @GuardedBy("lock") private boolean goAwaySent = false;
    @GuardedBy("lock") private Ping ping = null;
    @GuardedBy("lock") private boolean stopped = false;
    @GuardedBy("lock") private boolean hasStream = false;


    // require set
    protected WritableSocket writableSocket;

    @GuardedBy("lock")
    private final InUseStateAggregator<ClientStreamImpl> inUseState =
            new InUseStateAggregator<ClientStreamImpl>() {
                @Override
                protected void handleInUse() {
                    listener.transportInUse(true);
                }

                @Override
                protected void handleNotInUse() {
                    listener.transportInUse(false);
                }
            };

    private Listener listener = null;

    @GuardedBy("lock") private final HashMap<Integer, ClientStreamImpl> streams = new HashMap<>();

    public AbstractCgrpcClientTransport(
            ExecutorService transportExecutorService,
            ScheduledExecutorService scheduledExecutorService,
            int maxInboundMetadataSize,
            int maxInboundMessageSize,
            ClientTransportFactory.ClientTransportOptions options,
            SecurityLevel securityLevel,
            String authority,
            ClientListener<C> clientListener
    ) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.maxInboundMetadataSize = maxInboundMetadataSize;
        this.maxInboundMessageSize = maxInboundMessageSize;
        this.options = options;

        this.clientListener = clientListener;
        this.logId = InternalLogId.allocate(getClass(), authority);

        this.transportQueue = new OrderedQueue(
                transportExecutorService,
                (cmd) -> {}
        );

        this.attributes = Attributes.newBuilder()
                .set(GrpcAttributes.ATTR_CLIENT_EAG_ATTRS, options.getEagAttributes())
                .set(GrpcAttributes.ATTR_SECURITY_LEVEL, securityLevel) //TODO: Not sure about the right place.
                .build();
    }

    public void enableKeepAlive(
            boolean enable,
            long keepAliveTimeNanos,
            long keepAliveTimeoutNanos,
            boolean keepAliveWithoutCalls
    ) {
        this.enableKeepAlive = enable;
        this.keepAliveTimeNanos = keepAliveTimeNanos;
        this.keepAliveTimeoutNanos = keepAliveTimeoutNanos;
        this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    }

    //region WebSocket

    @Override
    public void goAway() {
        startGoAway(0, Status.Code.OK, Status.OK);
    }

    @Override
    public void sendHandshakeMessage(ByteBuffer byteBuffer) {
        ByteBuffer payload = ProtocolHelper.serializeHandshakeMessage(byteBuffer);
        this.writableSocket.send(payload);
    }
    //endregion

    @Override
    public Attributes getAttributes() {
        return this.attributes;
    }

    @Nullable
    @Override
    public Runnable start(Listener listener) {
        this.listener = Preconditions.checkNotNull(listener, "listener");

        if (enableKeepAlive) {
            keepAliveManager = new KeepAliveManager(
                    new KeepAliveManager.ClientKeepAlivePinger(this),
                    scheduledExecutorService,
                    keepAliveTimeNanos,
                    keepAliveTimeoutNanos,
                    keepAliveWithoutCalls
            );
            keepAliveManager.onTransportStarted();
        }

        return () -> {
            this.writableSocket.connect();
        };
    }

    @Override
    public void shutdown(Status reason) {
        synchronized (lock) {
            if (goAwayStatus != null) {
                return;
            }

            goAwayStatus = reason;
            listener.transportShutdown(goAwayStatus);
            stopIfNecessary();
        }
    }

    @Override
    public void shutdownNow(Status reason) {
        shutdown(reason);
        synchronized (lock) {
            Iterator<Map.Entry<Integer, ClientStreamImpl>> it = streams.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, ClientStreamImpl> entry = it.next();
                it.remove();
                entry.getValue().transportState().transportReportStatus(reason, false, new Metadata());
                maybeClearInUse(entry.getValue());
            }

            stopIfNecessary();
        }
    }

    @Override
    public ClientStream newStream(MethodDescriptor<?, ?> method, Metadata headers, CallOptions callOptions, ClientStreamTracer[] tracers) {
        Preconditions.checkNotNull(method, "method");
        Preconditions.checkNotNull(headers, "headers");
        StatsTraceContext statsTraceContext = StatsTraceContext.newClientContext(
                tracers, getAttributes(), headers
        );

        log.debug("newStream for {}", method);

        synchronized (this.lock) {
            TransportTracer transportTracer = new TransportTracer();
            ClientStreamImpl clientStream = new ClientStreamImpl(
                    frameWriter,
                    this,
                    lock,
                    writableBufferAllocator,
                    statsTraceContext,
                    transportTracer,
                    method,
                    headers,
                    callOptions
            );
            return clientStream;
        }
    }

    @GuardedBy("lock")
    public void streamReadyToStart(ClientStreamImpl clientStream, String fullMethodName, @Nullable byte[] payload) {
        if (goAwayStatus != null) {
            clientStream.transportState().transportReportStatus(
                    goAwayStatus, ClientStreamListener.RpcProgress.MISCARRIED, true, new Metadata());
//        } else if (streams.size() >= maxConcurrentStreams) {
//            pendingStreams.add(clientStream);
//            setInUse(clientStream);
        } else {
            startStream(clientStream, fullMethodName, payload);
        }
    }


    @GuardedBy("lock")
    private void startStream(ClientStreamImpl stream, String fullMethodName, @Nullable byte[] payload) {
        Preconditions.checkState(
                stream.transportState().id() == ClientStreamImpl.ABSENT_ID, "StreamId already assigned");

        int streamId = 0;
        try {
            streamId = this.streamIdGenerator.nextId();
        } catch (StreamIdOverflowException e) {
            startGoAway(Integer.MAX_VALUE, Status.OK.getCode(), Status.UNAVAILABLE.withDescription("Stream ids exhausted"));
            return ;
        }

        streams.put(streamId, stream);
        setInUse(stream);

        stream.transportState().start(streamId, fullMethodName, payload);
    }

    private void startGoAway(int lastKnownStreamId, @Nullable Status.Code errorCode, Status status) {
        synchronized (lock) {
            if (goAwayStatus == null) {
                goAwayStatus = status;
                listener.transportShutdown(status);
            }
            if (errorCode != null && !goAwaySent) {
                // Send GOAWAY with lastGoodStreamId of 0, since we don't expect any server-initiated
                // streams. The GOAWAY is part of graceful shutdown.
                goAwaySent = true;
                frameWriter.goAway(errorCode);
            }

            Iterator<Map.Entry<Integer, ClientStreamImpl>> it = streams.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, ClientStreamImpl> entry = it.next();
                if (entry.getKey() > lastKnownStreamId) {
                    it.remove();
                    entry.getValue().transportState().transportReportStatus(
                            status, ClientStreamListener.RpcProgress.REFUSED, false, new Metadata());
                    maybeClearInUse(entry.getValue());
                }
            }

            stopIfNecessary();
        }
    }

    public void finishStream(
            int streamId,
            @Nullable Status status,
            ClientStreamListener.RpcProgress rpcProgress,
            boolean stopDelivery,
            @Nullable Code errorCode,
            @Nullable Metadata trailers
    ) {
        synchronized (lock) {
            log.warn("Stream[{}]: finishStream", streamId);

            ClientStreamImpl stream = streams.remove(streamId);
            if (stream != null) {
                this.inUseState.updateObjectInUse(stream, false);
                if (errorCode != null) {
                    frameWriter.closeStream(streamId, Code.CANCELLED);
                }
                if (status != null) {
                    stream
                            .transportState()
                            .transportReportStatus(
                                    status,
                                    rpcProgress,
                                    stopDelivery,
                                    trailers != null ? trailers : new Metadata()
                            );
                }

                stopIfNecessary();
                maybeClearInUse(stream);
            }
        }
    }

    @Override
    public void ping(PingCallback callback, Executor executor) {
        Ping p = null;
        boolean writePing;
        synchronized (lock) {
            if (stopped) {
                return;
            }
            if (ping != null) {
                // we only allow one outstanding ping at a time, so just add the callback to
                // any outstanding operation
                p = ping;
                writePing = false;
            } else {
                Stopwatch stopwatch = Stopwatch.createStarted();
                stopwatch.start();
                p = ping = new Ping(0L, stopwatch);
                writePing = true;
            }
            if (writePing) {
                frameWriter.ping();
            }
        }
        // If transport concurrently failed/stopped since we released the lock above, this could
        // immediately invoke callback (which we shouldn't do while holding a lock)
        p.addCallback(callback, executor);
    }

    @Override
    public ListenableFuture<InternalChannelz.SocketStats> getStats() {
        return null;
    }

    @Override
    public InternalLogId getLogId() {
        return this.logId;
    }

    private void sendControlMessage(ControlType controlType, GeneratedMessageV3 message) {
        ByteBuffer sendBuffer = ProtocolHelper.serializeControlMessage(controlType, message);
        this.writableSocket.send(sendBuffer);
    }

    private void sendGrpcPayload(int streamId, @Nullable WritableBuffer frame, boolean endOfStream) {
        int size = 6;
        byte flags = 0;
        ByteBuffer readableBuffer = null;
        if (frame != null) {
            size += frame.readableBytes();
            readableBuffer = ((ByteBufferWritableBuffer) frame).buffer();
            ((Buffer)readableBuffer).flip();
        }
        if (endOfStream) {
            flags |= GrpcStreamFlag.EndOfFrame.getValue();
        }
        ByteBuffer sendBuffer = ByteBuffer.allocate(size)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(PayloadType.GRPC.getValue())
                .put(flags)
                .putInt(streamId);
        if (readableBuffer != null) {
            sendBuffer.put(readableBuffer);
        }
        ((Buffer)sendBuffer).flip();
        this.writableSocket.send(sendBuffer);
    }

    private ClientStream failedClientStream(
            final StatsTraceContext statsTraceCtx,
            final Status status
    ) {
        return new NoopClientStream() {
            @Override
            public void start(ClientStreamListener listener) {
                statsTraceCtx.clientOutboundHeaders();
                statsTraceCtx.streamClosed(status);
                listener.closed(status, ClientStreamListener.RpcProgress.PROCESSED, new Metadata());
            }
        };
    }

    /**
     * When the transport is in goAway state, we should stop it once all active streams finish.
     */
    @GuardedBy("lock")
    private void stopIfNecessary() {
        if (!(goAwayStatus != null && streams.isEmpty())) {
            return;
        }
        if (stopped) {
            return;
        }
        stopped = true;

        if (keepAliveManager != null) {
            keepAliveManager.onTransportTermination();
        }

        if (ping != null) {
            ping.failed(getPingFailure());
            ping = null;
        }

        if (!goAwaySent) {
            // Send GOAWAY with lastGoodStreamId of 0, since we don't expect any server-initiated
            // streams. The GOAWAY is part of graceful shutdown.
            goAwaySent = true;
            frameWriter.goAway(Status.Code.OK);
        }

        // We will close the underlying socket in the writing thread to break out the reader
        // thread, which will close the frameReader and notify the listener.
        frameWriter.close();
        listener.transportTerminated();
    }

    @GuardedBy("lock")
    private void maybeClearInUse(ClientStreamImpl stream) {
        if (hasStream) {
            if (streams.isEmpty()) {
                hasStream = false;
                if (keepAliveManager != null) {
                    // We don't have any active streams. No need to do keepalives any more.
                    // Again, we have to call this inside the lock to avoid the race between onTransportIdle
                    // and onTransportActive.
                    keepAliveManager.onTransportIdle();
                }
            }
        }
        if (stream.shouldBeCountedForInUse()) {
            inUseState.updateObjectInUse(stream, false);
        }
    }

    @GuardedBy("lock")
    private void setInUse(ClientStreamImpl stream) {
        if (!hasStream) {
            hasStream = true;
            if (keepAliveManager != null) {
                // We have a new stream. We might need to do keepalives now.
                // Note that we have to do this inside the lock to avoid calling
                // KeepAliveManager.onTransportActive and KeepAliveManager.onTransportIdle in the wrong
                // order.
                keepAliveManager.onTransportActive();
            }
        }
        if (stream.shouldBeCountedForInUse()) {
            inUseState.updateObjectInUse(stream, true);
        }
    }

    private final ProtocolHandler<Void> protocolHandler = new ProtocolHandler<Void>() {
        @Override
        public HandshakeState getHandshakeState() {
            return handshakeState;
        }

        @Override
        public void handleHandshakeMessage(Void unused, ByteBuffer payload) {
            clientListener.onHandshakeMessage((C) AbstractCgrpcClientTransport.this, payload);
        }

        @Override
        public void handleHandshakeResult(Void unused, HandshakeResult payload) {
            if (payload.getResolved()) {
                handshakeState = HandshakeState.COMPLETE;
            } else {
                handshakeState = HandshakeState.FAILURE;
            }
            clientListener.onHandshakeResult((C) AbstractCgrpcClientTransport.this, payload);
            if (payload.getResolved()) {
                listener.transportReady();
            } else {
                listener.transportShutdown(Status.PERMISSION_DENIED);
            }
        }

        @Override
        public void handleNewStream(Void unused, NewStream payload) {
            throw new RuntimeException("Never call");
        }

        @Override
        public void handleStreamHeader(Void unused, StreamHeader payload) {
            handleMetadata(payload.getStreamId(), false, payload.getHeadersList(), null);
        }

        @Override
        public void handleGrpcStream(Void unused, int streamId, EnumSet<GrpcStreamFlag> flags, ByteBuffer data) {
            ClientStreamImpl stream = getStream(streamId);
            if (stream == null) {
                if (mayHaveCreatedStream(streamId)) {
//                    synchronized (lock) {
//                        frameWriter.rstStream(streamId, ErrorCode.STREAM_CLOSED);
//                    }
                } else {
//                    onError(ErrorCode.PROTOCOL_ERROR, "Received data for unknown stream: " + streamId);
                    return;
                }
            } else {
                boolean endOfStream = flags.contains(GrpcStreamFlag.EndOfFrame);
                synchronized (lock) {
                    stream.transportState().transportDataReceived(data, endOfStream);
                }
            }
        }

        @Override
        public void handleCloseStream(Void unused, CloseStream payload) {
            Status status = ProtocolHelper.statusFromProto(payload.getStatus());
            handleMetadata(payload.getStreamId(), true, payload.getTrailersList(), status);
        }

        @Override
        public void handleFinishTransport(Void unused, FinishTransport payload) {
            log.trace("handleFinishTransport");

            Status status = Status.fromCodeValue(payload.getStatus().getCode())
                    .withDescription("Received Goaway");
            startGoAway(0, null, status);
        }

        private void handleMetadata(int streamId, boolean endOfStream, List<ByteString> metadataList, @Nullable Status status) {
            boolean unknownStream = false;
            Status failedStatus = null;

            Metadata metadata = ProtocolHelper.metadataDeserialize(metadataList);
            int metadataSize = MetadataUtils.metadataSize(metadata);
            if (metadataSize > maxInboundMetadataSize) {
                failedStatus = Status.RESOURCE_EXHAUSTED.withDescription(
                        String.format(
                                Locale.US,
                                "Response %s metadata larger than %d: %d",
                                "header",
                                maxInboundMetadataSize,
                                metadataSize));
                log.warn("metadataSize({}) > maxInboundMetadataSize({})", metadataSize, maxInboundMessageSize, failedStatus.asException());
            }

            synchronized (lock) {
                ClientStreamImpl stream = streams.get(streamId);
                if (stream == null) {
                    if (mayHaveCreatedStream(streamId)) {
                        // frameWriter.rstStream(streamId, ErrorCode.STREAM_CLOSED);
                    } else {
                        unknownStream = true;
                    }
                } else {
                    if (failedStatus == null) {
                        if (endOfStream) {
                            Preconditions.checkNotNull(status, "status");
                            stream.transportState().transportTrailersReceived(metadata, status);
                        } else {
                            stream.transportState().transportHeadersReceived(metadata);
                        }
                    } else {
                        if (!endOfStream) {
                            frameWriter.closeStream(streamId, Code.CANCELLED);
                        }
                        stream.transportState().transportReportStatus(failedStatus, false, new Metadata());
                    }
                }
            }
            if (unknownStream) {
                // We don't expect any server-initiated streams.
                // onError(ErrorCode.PROTOCOL_ERROR, "Received header for unknown stream: " + streamId);
                log.warn("Received header for unknown stream: " + streamId);
            }
        }
    };

    public final FrameWriter frameWriter = new FrameWriter() {
        @Override
        public void newStream(int streamId, List<ByteString> serializedMetadata, String fullMethodName, @Nullable byte[] payload) {
            NewStream.Builder builder = NewStream.newBuilder()
                    .setStreamId(streamId)
                    .addAllMetadata(serializedMetadata)
                    .setMethodName(fullMethodName);
            if (payload != null) {
                builder.setPayload(ByteString.copyFrom(payload));
            }
            NewStream newStream = builder.build();
            transportQueue.enqueue(() -> {
                sendControlMessage(ControlType.NewStream, newStream);
            }, true);
        }

        @Override
        public void data(int streamId, WritableBuffer frame, boolean endOfStream) {
            transportQueue.enqueue(() -> {
                sendGrpcPayload(streamId, frame, endOfStream);
            }, true);
        }

        @Override
        public void closeStream(int streamId, Code code) {
            CloseStream streamTrailers = CloseStream.newBuilder()
                    .setStreamId(streamId)
                    .setStatus(
                            com.google.rpc.Status.newBuilder()
                                    .setCode(code.getNumber())
                                    .build()
                    )
                    .build();
            transportQueue.enqueue(() -> {
                sendControlMessage(ControlType.CloseStream, streamTrailers);
            }, true);
        }

        @Override
        public void goAway(Status.Code code) {
            transportQueue.enqueue(() -> {
                FinishTransport finishTransport = FinishTransport.newBuilder()
                        .setStatus(ProtocolHelper.statusToProto(Status.fromCode(code)))
                        .build();
                sendControlMessage(ControlType.FinishTransport, finishTransport);
            }, true);
        }

        @Override
        public void close() {
            transportQueue.enqueue(() -> {
                writableSocket.close();
            }, true);
        }

        @Override
        public void ping() {
            transportQueue.enqueue(() -> {
                writableSocket.sendPing();
            }, true);
        }
    };

    ClientStreamImpl getStream(int streamId) {
        synchronized (lock) {
            return streams.get(streamId);
        }
    }

    private Throwable getPingFailure() {
        synchronized (lock) {
            if (goAwayStatus != null) {
                return goAwayStatus.asException();
            } else {
                return Status.UNAVAILABLE.withDescription("Connection closed").asException();
            }
        }
    }

    boolean mayHaveCreatedStream(int streamId) {
        synchronized (lock) {
            return streamId < streamIdGenerator.peekNextId() && (streamId & 1) == 1;
        }
    }

    @Override
    public void onOpen() {
        log.debug("[{}] onOpen", logId);
        synchronized (lock) {
            handshakeState = HandshakeState.HANDSHAKE;
        }
        clientListener.onConnected((C) AbstractCgrpcClientTransport.this);
    }

    @Override
    public void onMessage(ByteBuffer receiveBuffer) {
        try {
            if (keepAliveManager != null) {
                keepAliveManager.onDataReceived();
            }
            ProtocolHelper.handleMessage(protocolHandler, null, receiveBuffer);
        } catch (Exception e) {
            clientListener.onError((C) AbstractCgrpcClientTransport.this, e);
        }
    }

    @Override
    public void onClose() {
        log.debug("[{}] onClose", logId);
        clientListener.onClosed((C) AbstractCgrpcClientTransport.this);
        shutdownNow(Status.UNAVAILABLE);
    }

    @Override
    public void onError(Exception ex) {
        log.debug("[{}] onError", logId, ex);
        clientListener.onError((C) AbstractCgrpcClientTransport.this, ex);
        shutdownNow(Status.UNAVAILABLE); // TODO: When only CONNECT FAILURE
    }

    @Override
    public void onPing() {}

    @Override
    public void onPong() {
        synchronized (lock) {
            if (ping != null) {
                ping.complete();
                ping = null;
            }
        }
    }
}
