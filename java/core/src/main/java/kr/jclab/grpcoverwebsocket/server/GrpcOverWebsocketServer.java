package kr.jclab.grpcoverwebsocket.server;

import io.grpc.InternalChannelz;
import io.grpc.InternalInstrumented;
import io.grpc.ServerStreamTracer;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.internal.SharedResources;
import kr.jclab.grpcoverwebsocket.server.internal.ServerTransportImpl;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;

public class GrpcOverWebsocketServer implements InternalServer, GrpcOverWebsocketServerHandler {
    private final List<? extends ServerStreamTracer.Factory> streamTracerFactories;
    private final TransportTracer.Factory transportTracerFactory;
    private final GrpcOverWebsocketTransceiver transceiver;

    private final int maxInboundMetadataSize;
    private final int maxInboundMessageSize;

    private final Object lock = new Object();
    @GuardedBy("lock") private boolean started = false;
    @GuardedBy("lock") private boolean shutdown = false;
    @GuardedBy("lock") private boolean terminated = false;

    private final SocketAddress listenAddress;
    private ServerListener serverTransportListener;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService transportExecutorService;
    private final GrpcOverWebsocketServerHandler serverHandler;

    GrpcOverWebsocketServer(
            int maxInboundMetadataSize,
            int maxInboundMessageSize,
            List<? extends ServerStreamTracer.Factory> streamTracerFactories,
            TransportTracer.Factory transportTracerFactory,
            ScheduledExecutorService scheduledExecutorService,
            ExecutorService transportExecutorService,
            GrpcOverWebsocketTransceiver transceiver,
            GrpcOverWebsocketServerHandler serverHandler,
            SocketAddress listenAddress
    ) {
        this.maxInboundMetadataSize = maxInboundMetadataSize;
        this.maxInboundMessageSize = maxInboundMessageSize;

        this.streamTracerFactories = streamTracerFactories;
        this.transportTracerFactory = transportTracerFactory;

        this.scheduledExecutorService = scheduledExecutorService == null
                ? SharedResourceHolder.get(GrpcUtil.TIMER_SERVICE) : scheduledExecutorService;
        this.transportExecutorService = transportExecutorService == null
                ? SharedResourceHolder.get(SharedResources.TRANSPORT_EXECUTOR_SERVICE) : transportExecutorService;

        this.transceiver = transceiver;
        this.serverHandler = serverHandler;
        this.listenAddress = listenAddress;
    }

    @Override
    public void start(ServerListener serverListener) throws IOException {
        synchronized (this.lock) {
            checkState(!this.started, "Already started");
            checkState(!this.shutdown, "Shutting down");

            this.started = true;

            this.serverHandler.onStart(this);
            this.serverTransportListener = serverListener;
            this.transceiver.setDelegate(webSocketServerHandler);
        }
    }

    @Override
    public void shutdown() {
        synchronized (this.lock) {
            if (this.shutdown) {
                return ;
            }
            this.notifyShutdown();
        }
    }

    @Override
    public SocketAddress getListenSocketAddress() {
        return this.listenAddress;
    }

    @Nullable
    @Override
    public InternalInstrumented<InternalChannelz.SocketStats> getListenSocketStats() {
        return null;
    }

    @Override
    public List<? extends SocketAddress> getListenSocketAddresses() {
        return Collections.singletonList(getListenSocketAddress());
    }

    @Nullable
    @Override
    public List<InternalInstrumented<InternalChannelz.SocketStats>> getListenSocketStatsList() {
        return null;
    }

    public ServerTransportListener clientTransportCreated(ServerTransportImpl client) {
        ServerTransportListener serverTransportListener = this.serverTransportListener.transportCreated(client);
        return serverTransportListener;
    }

    private void notifyShutdown() {
        synchronized (this.lock) {
            if (this.shutdown) {
                return;
            }

            if (this.serverTransportListener != null) {
                this.serverTransportListener.serverShutdown();
            }

            this.shutdown = true;

            notifyTerminated();
        }
    }

    private void notifyTerminated() {
        synchronized (this.lock) {
            if (this.terminated) {
                return ;
            }

            this.terminated = true;

            this.serverHandler.onTerminated(this);
        }
    }

    private final ConcurrentHashMap<String, ServerTransportImpl> clients = new ConcurrentHashMap<>();
    private final WebsocketHandler webSocketServerHandler = new WebsocketHandler() {
        @Override
        public void afterConnectionEstablished(GrpcWebSocketSession session) throws Exception {
            ServerTransportImpl client = new ServerTransportImpl(
                    scheduledExecutorService,
                    transportExecutorService,
                    maxInboundMetadataSize,
                    maxInboundMessageSize,
                    streamTracerFactories,
                    transportTracerFactory.create(),
                    serverHandler,
                    GrpcOverWebsocketServer.this,
                    session
            );
            clients.put(session.getId(), client);
            serverHandler.onConnected(client.getHandshakeContext());
        }

        @Override
        public void handleMessage(GrpcWebSocketSession session, ByteBuffer message) throws Exception {
            ServerTransportImpl client = clients.get(session.getId());
            if (client == null) {
                throw new IOException("NO CLIENT");
            }
            client.handleReceive(message);
        }

        @Override
        public void afterConnectionClosed(GrpcWebSocketSession session) throws Exception {
            ServerTransportImpl client = clients.remove(session.getId());
            if (client == null) {
                throw new IOException("NO CLIENT");
            }
            serverHandler.onClosed(session);
            client.onClosedByRemote();
        }
    };
}
