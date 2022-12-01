package kr.jclab.grpcoverwebsocket.client;

import com.google.common.base.Preconditions;
import io.grpc.ChannelCredentials;
import io.grpc.ChannelLogger;
import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.internal.SharedResources;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.grpc.internal.GrpcUtil.DEFAULT_KEEPALIVE_TIMEOUT_NANOS;
import static io.grpc.internal.GrpcUtil.KEEPALIVE_TIME_NANOS_DISABLED;

public class GrpcOverWebsocketChannelBuilder extends AbstractManagedChannelImplBuilder<GrpcOverWebsocketChannelBuilder> {
    private final URI endpointUri;
    private final String authority;
    private final ManagedChannelImplBuilder managedChannelImplBuilder;

    private int maxInboundMetadataSize = GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE;
    private int maxInboundMessageSize = GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE;

    private long keepAliveTimeNanos = KEEPALIVE_TIME_NANOS_DISABLED;
    private long keepAliveTimeoutNanos = DEFAULT_KEEPALIVE_TIMEOUT_NANOS;
    private boolean keepAliveWithoutCalls = false;

    private ScheduledExecutorService scheduledExecutorService;
    private ExecutorService transportExecutorService;
    private WebsocketClientListener<GrpcOverWebsocketClientTransport> clientListener = new WebsocketClientListener<GrpcOverWebsocketClientTransport>() {};

    GrpcOverWebsocketChannelBuilder(URI endpointUri) {
        this.endpointUri = endpointUri;
        this.authority = uriToAuthority(endpointUri);
        this.managedChannelImplBuilder = new ManagedChannelImplBuilder(
                this.authority,
                new GrpcOverWebsocketTransportFactoryBuilder(),
                null
        );
    }

    @Override
    public GrpcOverWebsocketChannelBuilder keepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
        Preconditions.checkArgument(keepAliveTime > 0L, "keepalive time must be positive");
        keepAliveTimeNanos = timeUnit.toNanos(keepAliveTime);
        keepAliveTimeNanos = KeepAliveManager.clampKeepAliveTimeInNanos(keepAliveTimeNanos);
        return this;
    }

    @Override
    public GrpcOverWebsocketChannelBuilder keepAliveTimeout(long keepAliveTimeout, TimeUnit timeUnit) {
        Preconditions.checkArgument(keepAliveTimeout > 0L, "keepalive timeout must be positive");
        keepAliveTimeoutNanos = timeUnit.toNanos(keepAliveTimeout);
        keepAliveTimeoutNanos = KeepAliveManager.clampKeepAliveTimeoutInNanos(keepAliveTimeoutNanos);
        return this;
    }

    @Override
    public GrpcOverWebsocketChannelBuilder keepAliveWithoutCalls(boolean enable) {
        keepAliveWithoutCalls = enable;
        return this;
    }

    @Override
    public GrpcOverWebsocketChannelBuilder maxInboundMetadataSize(int bytes) {
        super.maxInboundMetadataSize(bytes);
        this.maxInboundMetadataSize = bytes;
        return this;
    }

    @Override
    public GrpcOverWebsocketChannelBuilder maxInboundMessageSize(int bytes) {
        super.maxInboundMessageSize(bytes);
        this.maxInboundMessageSize = bytes;
        return this;
    }

    private static String uriToAuthority(URI endpointUri) {
        try {
            return new URI(null, null, endpointUri.getHost(), endpointUri.getPort(), null, null, null).getAuthority();
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("Invalid host or port: " + endpointUri.getHost() + " " + endpointUri.getPort(), ex);
        }
    }

    public static GrpcOverWebsocketChannelBuilder forTarget(String uri) {
        try {
            return new GrpcOverWebsocketChannelBuilder(new URI(uri));
        } catch (URISyntaxException e) {
            throw new RuntimeException();
        }
    }

    public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void setTransportExecutorService(ExecutorService transportExecutorService) {
        this.transportExecutorService = transportExecutorService;
    }

    public void setClientListener(WebsocketClientListener<GrpcOverWebsocketClientTransport> clientListener) {
        this.clientListener = clientListener;
    }

    @Override
    protected ManagedChannelBuilder<?> delegate() {
        return this.managedChannelImplBuilder;
    }

    ClientTransportFactory buildTransportFactory() {
        return new GrpcOverWebsocketClientTransportFactory(
                scheduledExecutorService,
                transportExecutorService,
                keepAliveTimeNanos != KEEPALIVE_TIME_NANOS_DISABLED,
                keepAliveTimeNanos,
                keepAliveTimeoutNanos,
                keepAliveWithoutCalls
        );
    }

    private final class GrpcOverWebsocketTransportFactoryBuilder implements ManagedChannelImplBuilder.ClientTransportFactoryBuilder {
        @Override
        public ClientTransportFactory buildClientTransportFactory() {
            return buildTransportFactory();
        }
    }

    /**
     * Creates InProcess transports. Exposed for internal use, as it should be private.
     */
    private final class GrpcOverWebsocketClientTransportFactory implements ClientTransportFactory {
        private final ScheduledExecutorService timerService;
        private final ExecutorService transportExecutorService;
        private final boolean useSharedTimer;
        private final boolean useSharedTransportExecutorService;
        private boolean closed;

        private final boolean enableKeepAlive;
        private final long keepAliveTimeNanos;
        private final long keepAliveTimeoutNanos;
        private final boolean keepAliveWithoutCalls;

        private GrpcOverWebsocketClientTransportFactory(
                @Nullable ScheduledExecutorService scheduledExecutorService,
                @Nullable ExecutorService transportExecutorService,
                boolean enableKeepAlive,
                long keepAliveTimeNanos,
                long keepAliveTimeoutNanos,
                boolean keepAliveWithoutCalls
        ) {
            this.enableKeepAlive = enableKeepAlive;
            this.keepAliveTimeNanos = keepAliveTimeNanos;
            this.keepAliveTimeoutNanos = keepAliveTimeoutNanos;
            this.keepAliveWithoutCalls = keepAliveWithoutCalls;

            this.useSharedTimer = scheduledExecutorService == null;
            this.timerService = useSharedTimer
                    ? SharedResourceHolder.get(GrpcUtil.TIMER_SERVICE) : scheduledExecutorService;

            this.useSharedTransportExecutorService = scheduledExecutorService == null;
            this.transportExecutorService = useSharedTransportExecutorService
                    ? SharedResourceHolder.get(SharedResources.TRANSPORT_EXECUTOR_SERVICE) : transportExecutorService;
        }

        @Override
        public ConnectionClientTransport newClientTransport(
                SocketAddress addr,
                ClientTransportOptions options,
                ChannelLogger channelLogger
        ) {
            if (this.closed) {
                throw new IllegalStateException("The transport factory is closed.");
            }

            GrpcOverWebsocketClientTransport transport = new GrpcOverWebsocketClientTransport(
                    this.transportExecutorService,
                    this.timerService,
                    maxInboundMetadataSize,
                    maxInboundMessageSize,
                    options,
                    endpointUri,
                    authority,
                    clientListener
            );
            if (enableKeepAlive) {
                transport.enableKeepAlive(true, keepAliveTimeNanos, keepAliveTimeoutNanos, keepAliveWithoutCalls);
            }
            return transport;
        }

        @Override
        public ScheduledExecutorService getScheduledExecutorService() {
            return timerService;
        }

        @Override
        public SwapChannelCredentialsResult swapChannelCredentials(ChannelCredentials channelCreds) {
            return null;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (useSharedTimer) {
                SharedResourceHolder.release(GrpcUtil.TIMER_SERVICE, timerService);
            }
            if (useSharedTransportExecutorService) {
                SharedResourceHolder.release(SharedResources.TRANSPORT_EXECUTOR_SERVICE, transportExecutorService);
            }
        }
    }
}
