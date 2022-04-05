package kr.jclab.grpcoverwebsocket.client;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.*;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.client.command.GracefulCloseCommand;
import kr.jclab.grpcoverwebsocket.client.internal.ClientStreamImpl;
import kr.jclab.grpcoverwebsocket.client.internal.Ping;
import kr.jclab.grpcoverwebsocket.core.protocol.v1.*;
import kr.jclab.grpcoverwebsocket.internal.*;
import kr.jclab.grpcoverwebsocket.protocol.v1.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.framing.Framedata;
import org.java_websocket.handshake.ServerHandshake;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

@Slf4j
public class GrpcOverWebsocketClientTransport implements
        ConnectionClientTransport,
        GrpcOverWebsocketClientConnection,
        ClientTransportLifecycleManager.LifecycleManagerListener
{
    private final int maxInboundMetadataSize;
    private final int maxInboundMessageSize;
    @Getter
    private final ClientTransportFactory.ClientTransportOptions options;

    @GuardedBy("lock") private ClientTransportLifecycleManager lifecycleManager;
    @Getter
    private final OrderedQueue transportQueue;

    private final URI endpointUri;
    private final ClientListener clientListener;
    private final InternalLogId logId;

    private final StreamIdGenerator streamIdGenerator = new StreamIdGenerator(StreamIdGenerator.Mode.Client);
    private final WritableBufferAllocator writableBufferAllocator = new ByteBufferWritableBufferAllocator();
    private final Attributes attributes;

    private final Object lock = new Object();
    private final WebSocketClient webSocketClient;
    @GuardedBy("lock") private HandshakeState handshakeState = HandshakeState.HANDSHAKE;
    @GuardedBy("lock") private Ping ping = null;

    @GuardedBy("lock")
    private final InUseStateAggregator<ClientStreamImpl> inUseState =
            new InUseStateAggregator<ClientStreamImpl>() {
                @Override
                protected void handleInUse() {
                    lifecycleManager.notifyInUse(true);
                }

                @Override
                protected void handleNotInUse() {
                    lifecycleManager.notifyInUse(false);
                }
            };

    private Listener transportListener = null;

    private final ConcurrentHashMap<Integer, ClientStreamImpl> streams = new ConcurrentHashMap<>();

    public GrpcOverWebsocketClientTransport(
            ExecutorService transportExecutorService,
            int maxInboundMetadataSize,
            int maxInboundMessageSize,
            ClientTransportFactory.ClientTransportOptions options,
            URI endpointUri,
            String authority,
            ClientListener clientListener
    ) {
        this.maxInboundMetadataSize = maxInboundMetadataSize;
        this.maxInboundMessageSize = maxInboundMessageSize;
        this.options = options;

        this.endpointUri = endpointUri;
        this.clientListener = clientListener;
        this.logId = InternalLogId.allocate(getClass(), authority);

        this.webSocketClient = new WebSocketClient(
                this.endpointUri
        ) {
            @Override
            public void onOpen(ServerHandshake handshakedata) {
                log.info("[{}] onOpen", logId);
                synchronized (lock) {
                    handshakeState = HandshakeState.HANDSHAKE;
                }
                clientListener.onConnected(GrpcOverWebsocketClientTransport.this);
            }

            @Override
            public void onMessage(String message) {
                throw new RuntimeException("Never called");
            }

            @Override
            public void onMessage(ByteBuffer receiveBuffer) {
                try {
                    ProtocolHelper.handleMessage(protocolHandler, null, receiveBuffer);
                } catch (Exception e) {
                    clientListener.onError(GrpcOverWebsocketClientTransport.this, e);
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.info("[{}] onClose", logId);
                clientListener.onClosed(GrpcOverWebsocketClientTransport.this);
//
//                if (enableReconnect) {
//                    scheduledExecutorService.schedule(() -> {
//                        if (listener.beforeReconnect()) {
//                            reconnect();
//                        }
//                    }, 3000, TimeUnit.MILLISECONDS);
//                }
            }

            @Override
            public void onError(Exception ex) {
                log.info("[{}] onError", logId, ex);
                clientListener.onError(GrpcOverWebsocketClientTransport.this, ex);
                shutdownNow(Status.UNAVAILABLE); // TODO: When only CONNECT FAILURE
            }

            @Override
            public void onWebsocketPing(WebSocket conn, Framedata f) {
                super.onWebsocketPing(conn, f);
            }

            @Override
            public void onWebsocketPong(WebSocket conn, Framedata f) {
                super.onWebsocketPong(conn, f);
                synchronized (lock) {
                    ping.complete();
                    ping = null;
                }
            }
        };

        this.transportQueue = new OrderedQueue(
                transportExecutorService,
                (cmd) -> {
                    if (cmd instanceof GracefulCloseCommand) {
                        gracefulClose((GracefulCloseCommand) cmd);
                    }
                }
        );

        this.attributes = Attributes.newBuilder()
                .set(GrpcAttributes.ATTR_CLIENT_EAG_ATTRS, options.getEagAttributes())
                .build();
    }

    @Override
    public Attributes getAttributes() {
        return this.attributes;
    }

    @Nullable
    @Override
    public Runnable start(Listener listener) {
        return () -> {
            this.webSocketClient.connect();
            this.transportListener = listener;
            this.lifecycleManager = new ClientTransportLifecycleManager(
                    this,
                    listener
            );
        };
    }

    @Override
    public void shutdown(Status reason) {
        this.transportQueue.enqueue(new GracefulCloseCommand(reason), true);
    }

    @Override
    public void shutdownNow(Status reason) {
        synchronized (this.lock) {
            this.lifecycleManager.notifyShutdown(reason);
            if (this.lifecycleManager.transportTerminated()) {
                return ;
            }

            cancelPing(new IOException("shutdown"));

            log.info("all streams shutdown by shutdownNow");

            finishTransport(reason);
        }
    }

    private void cancelStreamsLocally(Status reason) {
        Iterator<Map.Entry<Integer, ClientStreamImpl>> it = this.streams.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ClientStreamImpl> entry = it.next();
            it.remove();
            entry.getValue().closeByForcelly(reason);
            this.inUseState.updateObjectInUse(entry.getValue(), false);
        }
    }

    @Override
    public ClientStream newStream(MethodDescriptor<?, ?> method, Metadata headers, CallOptions callOptions, ClientStreamTracer[] tracers) {
        StatsTraceContext statsTraceContext = StatsTraceContext.newClientContext(
                tracers, getAttributes(), headers
        );

        log.info("newStream for {}", method);

        synchronized (this.lock) {
            Status shutdownStatus = this.lifecycleManager.getShutdownStatus();
            if (shutdownStatus != null) {
                return this.failedClientStream(statsTraceContext, shutdownStatus);
            }

            TransportTracer transportTracer = new TransportTracer();
            ClientStreamImpl clientStream = new ClientStreamImpl(
                    this,
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

    public void startStream(ClientStreamImpl stream) {
        int streamId = 0;
        try {
            streamId = this.streamIdGenerator.nextId();
        } catch (StreamIdOverflowException e) {
            goAway();
            return ;
        }
        this.streams.put(streamId, stream);
        this.inUseState.updateObjectInUse(stream, true);
        stream.start(streamId);
    }

    public void finishStream(int streamId) {
        log.warn("Stream[{}]: finishStream", streamId);

        ClientStreamImpl stream = streams.remove(streamId);
        if (stream == null) {
            log.warn("Invalid stream id: " + streamId);
            throw new RuntimeException("Invalid stream id: " + streamId);
        }

        this.inUseState.updateObjectInUse(stream, false);

        if (this.lifecycleManager.getShutdownStatus() != null) {
            notifyTerminateIfNoStream();
        }
    }

    @Override
    public void ping(PingCallback callback, Executor executor) {
        //TODO: verify channel status in transport queue

        synchronized (this.lock) {
            if (this.lifecycleManager.transportTerminated()) {
                callback.onFailure(this.lifecycleManager.getShutdownThrowable());
                return ;
            }
            sendPingFrameTraced(callback, executor);
        }
    }

    /**
     * Sends a PING frame. If a ping operation is already outstanding, the callback in the message is
     * registered to be called when the existing operation completes, and no new frame is sent.
     */
    @GuardedBy("lock")
    private void sendPingFrameTraced(PingCallback callback, Executor executor) {
        // we only allow one outstanding ping at a time, so just add the callback to
        // any outstanding operation
        if (this.ping != null) {
            this.ping.addCallback(callback, executor);
            return;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        this.ping = new Ping(0L, stopwatch);
        this.ping.addCallback(callback, executor);
        this.webSocketClient.sendPing();
    }

    @GuardedBy("lock")
    private void cancelPing(Throwable e) {
        if (this.ping != null) {
            this.ping.failed(e);
            this.ping = null;
        }
    }

    @Override
    public ListenableFuture<InternalChannelz.SocketStats> getStats() {
        return null;
    }

    @Override
    public InternalLogId getLogId() {
        return this.logId;
    }

    @Override
    public void goAway() {
        this.shutdown(Status.OK);
    }

    public void sendControlMessage(ControlType controlType, GeneratedMessageV3 message) {
        if (this.lifecycleManager.transportTerminated()) {
            return ;
        }

        ByteBuffer sendBuffer = ProtocolHelper.serializeControlMessage(controlType, message);
        this.webSocketClient.send(sendBuffer);
    }

    public void sendGrpcPayload(int streamId, @Nullable WritableBuffer frame, boolean endOfStream) {
        if (this.lifecycleManager.transportTerminated()) {
            return ;
        }

        int size = 18;
        byte flags = 0;
        ByteBuffer readableBuffer = null;
        if (frame != null) {
            size += frame.readableBytes();
            readableBuffer = ((ByteBufferWritableBuffer) frame).buffer();
            readableBuffer.flip();
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
        sendBuffer.flip();
        this.webSocketClient.send(sendBuffer);
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

    private void gracefulClose(GracefulCloseCommand cmd) {
        this.lifecycleManager.notifyShutdown(cmd.getReason());
    }

    private final ProtocolHandler<Void> protocolHandler = new ProtocolHandler<Void>() {
        @Override
        public HandshakeState getHandshakeState() {
            return handshakeState;
        }

        @Override
        public void handleHandshakeMessage(Void unused, ByteBuffer payload) {
            clientListener.onHandshakeMessage(GrpcOverWebsocketClientTransport.this, payload);
        }

        @Override
        public void handleHandshakeResult(Void unused, HandshakeResult payload) {
            if (payload.getResolved()) {
                handshakeState = HandshakeState.COMPLETE;
            } else {
                handshakeState = HandshakeState.FAILURE;
            }
            clientListener.onHandshakeResult(GrpcOverWebsocketClientTransport.this, payload);
            if (payload.getResolved()) {
                transportListener.transportReady();
            } else {
                transportListener.transportShutdown(Status.PERMISSION_DENIED);
            }
        }

        @Override
        public void handleNewStream(Void unused, NewStream payload) {
            throw new RuntimeException("Never call");
        }

        @Override
        public void handleStreamHeader(Void unused, StreamHeader payload) {
            ClientStreamImpl stream = streams.get(payload.getStreamId());
            if (stream == null) {
                log.warn("Invalid stream id: " + payload.getStreamId());
                throw new RuntimeException("Invalid stream id: " + payload.getStreamId());
            }
            Metadata metadata = ProtocolHelper.metadataDeserialize(payload.getHeadersList());
            //TODO: more efficiently
            int metadataSize = MetadataUtils.metadataSize(metadata);
            if (metadataSize > maxInboundMetadataSize) {
                log.warn("metadataSize({}) > maxInboundMetadataSize({})", metadataSize, maxInboundMessageSize);
                stream.cancel(Status.RESOURCE_EXHAUSTED);
                return ;
            }
            stream.handleStreamHeader(metadata);
        }

        @Override
        public void handleGrpcStream(Void unused, int streamId, EnumSet<GrpcStreamFlag> flags, ByteBuffer data) {
            ClientStreamImpl stream = streams.get(streamId);
            if (stream == null) {
                throw new RuntimeException("Invalid stream id: " + streamId);
            }
            stream.handleGrpcStream(flags, data);
        }

        @Override
        public void handleCloseStream(Void unused, CloseStream payload) {
            ClientStreamImpl stream = streams.get(payload.getStreamId());
            if (stream == null) {
                throw new RuntimeException("Invalid stream id: " + payload.getStreamId());
            }

            Status status = ProtocolHelper.statusFromProto(payload.getStatus());
            Metadata trailers = ProtocolHelper.metadataDeserialize(payload.getTrailersList());

            if (MetadataUtils.metadataSize(trailers) > maxInboundMetadataSize) {
                stream.cancel(Status.RESOURCE_EXHAUSTED);
            } else {
                stream.handleCloseStream(status, trailers);
            }

            finishStream(stream.getStreamId());
        }

        @Override
        public void handleFinishTransport(Void unused, FinishTransport payload) {
            log.info("handleFinishTransport");
            finishTransport(ProtocolHelper.statusFromProto(payload.getStatus()));
        }
    };

    @Override
    public void afterShutdown() {
        log.info("afterShutdown");
        this.notifyTerminateIfNoStream();
    }

    @Override
    public void afterTerminate() {
        log.info("afterTerminate");
        this.webSocketClient.close();
    }

    private void notifyTerminateIfNoStream() {
        if (this.streams.isEmpty() && this.lifecycleManager.getShutdownStatus() != null) {
            this.finishTransport(Status.OK);
        }
    }

    private void finishTransport(Status status) {
        try {
            cancelStreamsLocally(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.lifecycleManager.notifyTerminated(status);
    }
}
