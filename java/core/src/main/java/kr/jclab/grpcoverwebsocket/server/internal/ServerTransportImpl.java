package kr.jclab.grpcoverwebsocket.server.internal;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.*;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.core.protocol.v1.*;
import kr.jclab.grpcoverwebsocket.internal.*;
import kr.jclab.grpcoverwebsocket.protocol.v1.*;
import kr.jclab.grpcoverwebsocket.server.*;
import kr.jclab.grpcoverwebsocket.server.command.CancelServerStreamCommand;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class ServerTransportImpl implements
        ProtocolHandler<Void>,
        ServerTransport,
        ServerTransportLifecycleManager.LifecycleManagerListener {
    private final WritableBufferAllocator writableBufferAllocator = new ByteBufferWritableBufferAllocator();

    private final ScheduledExecutorService scheduledExecutorService;

    private final int maxInboundMetadataSize;
    private final int maxInboundMessageSize;
    private final String authority;

    private final List<? extends ServerStreamTracer.Factory> streamTracerFactories;
    private final TransportTracer transportTracer;
    private final GrpcOverWebsocketServerHandler connectionHandler;
    private final GrpcWebSocketSession session;
    private final GrpcOverWebsocketServer server;
    private final InternalLogId logId;

    private HandshakeState handshakeState = HandshakeState.HANDSHAKE;
    private ServerTransportLifecycleManager lifecycleManager = null;
    private ServerTransportListener serverTransportListener = null;

    private final Attributes transportAttributes;
    private Attributes attributes = null;

    private final Object lock = new Object();
    private final ConcurrentHashMap<Integer, ServerStreamImpl> virtualStreams = new ConcurrentHashMap<>();

    @Getter
    private final OrderedQueue transportQueue;

    public ServerTransportImpl(
            ScheduledExecutorService scheduledExecutorService,
            ExecutorService transportExecutorService,
            int maxInboundMetadataSize,
            int maxInboundMessageSize,
            List<? extends ServerStreamTracer.Factory> streamTracerFactories,
            TransportTracer transportTracer,
            GrpcOverWebsocketServerHandler connectionHandler,
            GrpcOverWebsocketServer server,
            GrpcWebSocketSession session
    ) {
        this.scheduledExecutorService = scheduledExecutorService;

        this.maxInboundMetadataSize = maxInboundMetadataSize;
        this.maxInboundMessageSize = maxInboundMessageSize;

        this.streamTracerFactories = streamTracerFactories;
        this.transportTracer = transportTracer;
        this.connectionHandler = connectionHandler;
        this.session = session;
        this.authority = session.getAuthority();
        this.server = server;
        this.logId = InternalLogId.allocate(getClass(), session.getId());
        this.transportQueue = new OrderedQueue(
                transportExecutorService,
                (cmd) -> {
                    if (cmd instanceof CancelServerStreamCommand) {
                        CancelServerStreamCommand cmdImpl = (CancelServerStreamCommand) cmd;
                        cmdImpl.getStream().cancelStream(cmdImpl.getStatus(), cmdImpl.isRemote());
                    }
                }
        );

        this.transportAttributes = Attributes.newBuilder()
                .set(Grpc.TRANSPORT_ATTR_LOCAL_ADDR, session.getLocalAddress())
                .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, session.getRemoteAddress())
                .build();
    }
    
    public void onClosedByRemote() {
        log.info("onClosedByRemote");
        synchronized (this.lock) {
            this.lifecycleManager.notifyTerminated(Status.UNKNOWN);
        }
    }

    public void handleReceive(ByteBuffer receiveBuffer) throws InvalidProtocolBufferException {
        ProtocolHelper.handleMessage(this, null, receiveBuffer);
    }

    @Getter
    private final HandshakeContext handshakeContext = new HandshakeContext() {
        @Override
        public ScheduledExecutorService getScheduledExecutorService() {
            return scheduledExecutorService;
        }

        @Override
        public GrpcWebSocketSession getSession() {
            return session;
        }

        @Override
        public void ready(String metadata) {
            checkState(handshakeState == HandshakeState.HANDSHAKE, "already handshaked");
            handshakeState = HandshakeState.COMPLETE;

            log.debug("Client[{}] handshake ready", session.getId());

            HandshakeResult handshakeResult = HandshakeResult.newBuilder()
                    .setResolved(true)
                    .setMetadata(Optional.ofNullable(metadata).orElse(""))
                    .build();
            try {
                session.sendMessage(ProtocolHelper.serializeControlMessage(ControlType.HandshakeResult, handshakeResult));
            } catch (IOException e) {
                connectionHandler.onError(session, e);
            }

            serverTransportListener = server.clientTransportCreated(ServerTransportImpl.this);
            lifecycleManager = new ServerTransportLifecycleManager(ServerTransportImpl.this, serverTransportListener);

            attributes = serverTransportListener.transportReady(transportAttributes);
        }

        @Override
        public void reject(String message, String metadata) {
            checkState(handshakeState == HandshakeState.HANDSHAKE, "already handshaked");
            handshakeState = HandshakeState.FAILURE;

            log.debug("Client[{}] handshake reject", session.getId());

            HandshakeResult handshakeResult = HandshakeResult.newBuilder()
                    .setResolved(false)
                    .setMessage(message)
                    .setMetadata(Optional.ofNullable(metadata).orElse(""))
                    .build();
            try {
                session.sendMessage(ProtocolHelper.serializeControlMessage(ControlType.HandshakeResult, handshakeResult));
            } catch (IOException e) {
                connectionHandler.onError(session, e);
            }
        }

        @Override
        public void sendMessage(ByteBuffer data) {
            ByteBuffer sendBuffer = ByteBuffer.allocate(1 + data.remaining())
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .put(PayloadType.HANDSHAKE.getValue())
                    .put(data);
            sendBuffer.flip();
            try {
                session.sendMessage(sendBuffer);
            } catch (IOException e) {
                connectionHandler.onError(session, e);
            }
        }
    };

    @Override
    public HandshakeState getHandshakeState() {
        return handshakeState;
    }

    @Override
    public void handleHandshakeMessage(Void unused, ByteBuffer payload) {
        connectionHandler.onHandshakeMessage(handshakeContext, payload);
    }

    @Override
    public void handleHandshakeResult(Void unused, HandshakeResult payload) {
        throw new RuntimeException("never called");
    }

    @Override
    public void handleNewStream(Void unused, NewStream payload) {
        int streamId = payload.getStreamId();
        log.info("Client[{}] new stream {}", this.session.getId(), streamId);

        byte[][] serializedMetadata = payload.getMetadataList()
                .stream()
                .map(ByteString::toByteArray)
                .toArray(byte[][]::new);
        int metadataSize = MetadataUtils.metadataSize(serializedMetadata);

        if (metadataSize > this.maxInboundMetadataSize) {
            CloseStream closeStream = CloseStream.newBuilder()
                    .setStreamId(streamId)
                    .setStatus(ProtocolHelper.statusToProto(Status.fromCode(Status.Code.RESOURCE_EXHAUSTED)))
                    .build();
            sendControlMessage(ControlType.CloseStream, closeStream);
            return ;
        }

        Metadata metadata = InternalMetadata.newMetadata(serializedMetadata);

        StatsTraceContext statsTraceContext = StatsTraceContext.newServerContext(
                streamTracerFactories,
                payload.getMethodName(),
                metadata
        );
        ServerStreamImpl stream = new ServerStreamImpl(
                writableBufferAllocator,
                authority,
                attributes,
                statsTraceContext,
                this.transportTracer,
                this,
                streamId
        );
        this.virtualStreams.put(streamId, stream);
        this.serverTransportListener.streamCreated(stream, payload.getMethodName(), metadata);
        stream.start();
    }

    @Override
    public void handleStreamHeader(Void unused, StreamHeader payload) {
        throw new RuntimeException("never call");
    }

    @Override
    public void handleGrpcStream(Void unused, int streamId, EnumSet<GrpcStreamFlag> flags, ByteBuffer data) {
        ServerStreamImpl stream = this.virtualStreams.get(streamId);
        if (stream == null) {
            log.error("handleGrpcStream: Invalid stream id: " + streamId);
            return ;
        }
        log.debug("Client[{}, stream={}] handleGrpcStream: {} bytes (flags: {})", this.session.getId(), streamId, data.remaining(), flags);
        stream.handlePayload(flags, data);
    }

    @Override
    public void handleCloseStream(Void unused, CloseStream payload) {
        int streamId = payload.getStreamId();
        ServerStreamImpl stream = this.virtualStreams.remove(streamId);
        if (stream == null) {
            log.error("handleCloseStream: Invalid stream id: " + streamId);
            return ;
        }
        Status status = ProtocolHelper.statusFromProto(payload.getStatus());
        log.info("Client[{}, stream={}] close by client: {}", this.session.getId(), streamId, status.getCode());
        transportQueue.enqueue(new CancelServerStreamCommand(stream, true, status), true);
    }

    @Override
    public void handleFinishTransport(Void unused, FinishTransport payload) {

    }

    @Override
    public void shutdown() {
        log.debug("shutdown");
        synchronized (this.lock) {
            if (this.lifecycleManager != null) {
                this.lifecycleManager.notifyShutdown(Status.OK);
            }
        }
    }

    @Override
    public void shutdownNow(Status reason) {
        log.debug("shutdownNow");
        synchronized (this.lock) {
            if (this.lifecycleManager == null) {
                return ;
            }

            if (this.lifecycleManager.transportTerminated()) {
                return ;
            }

            // TODO: Its needed?
            Iterator<Map.Entry<Integer, ServerStreamImpl>> it = this.virtualStreams.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, ServerStreamImpl> entry = it.next();
                it.remove();
                entry.getValue().cancelStream(reason, true);
            }

            this.session.close();
            this.lifecycleManager.notifyTerminated(reason);
        }
    }

    @Override
    public ScheduledExecutorService getScheduledExecutorService() {
        return this.scheduledExecutorService;
    }

    @Override
    public ListenableFuture<InternalChannelz.SocketStats> getStats() {
        return null;
    }

    @Override
    public InternalLogId getLogId() {
        return this.logId;
    }

    public void sendControlMessage(ControlType controlType, GeneratedMessageV3 message) {
        log.debug("Client[{}] sendControlMessage {}", this.session.getId(), controlType);
        ByteBuffer sendBuffer = ProtocolHelper.serializeControlMessage(controlType, message);
        try {
            this.session.sendMessage(sendBuffer);
        } catch (IOException e) {
            this.connectionHandler.onError(session, e);
        }
    }

    public void sendGrpcPayload(int streamId, @Nullable WritableBuffer frame) {
        int size = 6;
        byte flags = 0;
        ByteBuffer readableBuffer = null;
        if (frame != null) {
            size += frame.readableBytes();
            readableBuffer = ((ByteBufferWritableBuffer) frame).buffer();
            readableBuffer.flip();
        }
//        if (endOfStream) {
//            flags |= GrpcStreamFlag.EndOfFrame.getValue();
//        }

        log.debug("Client[{}, stream={}] sendGrpcPayload {} bytes", this.session.getId(), streamId, size - 6);
        ByteBuffer sendBuffer = ByteBuffer.allocate(size)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(PayloadType.GRPC.getValue())
                .put(flags)
                .putInt(streamId);
        if (readableBuffer != null) {
            sendBuffer.put(readableBuffer);
        }
        sendBuffer.flip();
        try {
            this.session.sendMessage(sendBuffer);
        } catch (IOException e) {
            this.connectionHandler.onError(session, e);
        }
    }

    @Override
    public void afterShutdown() {
        if (!this.lifecycleManager.transportTerminated()) {
            FinishTransport finishTransport = FinishTransport.newBuilder()
                    .setStatus(ProtocolHelper.statusToProto(this.lifecycleManager.getShutdownStatus()))
                    .build();
            sendControlMessage(ControlType.FinishTransport, finishTransport);
        }

        this.lifecycleManager.notifyTerminated(this.lifecycleManager.getShutdownStatus());
    }

    @Override
    public void afterTerminate() {
    }
}
