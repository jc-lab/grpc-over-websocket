package kr.jclab.grpcoverwebsocket.client;

import com.google.errorprone.annotations.DoNotCall;
import io.grpc.ChannelCredentials;
import io.grpc.ChannelLogger;
import io.grpc.ManagedChannelBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessSocketAddress;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.internal.SharedResources;
import kr.jclab.grpcoverwebsocket.server.GrpcOverWebsocketServerBuilder;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class GrpcOverWebsocketChannelBuilder extends AbstractManagedChannelImplBuilder<GrpcOverWebsocketChannelBuilder> {
    private final URI endpointUri;
    private final String authority;
    private final ManagedChannelImplBuilder managedChannelImplBuilder;

    private int maxInboundMetadataSize = GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE;
    private int maxInboundMessageSize = GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE;

    private ScheduledExecutorService scheduledExecutorService;
    private ExecutorService transportExecutorService;
    private ClientListener clientListener = new ClientListener() {};

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

    public void setClientListener(ClientListener clientListener) {
        this.clientListener = clientListener;
    }

    @Override
    protected ManagedChannelBuilder<?> delegate() {
        return this.managedChannelImplBuilder;
    }

    ClientTransportFactory buildTransportFactory() {
        return new GrpcOverWebsocketClientTransportFactory(
                scheduledExecutorService,
                transportExecutorService
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

        private GrpcOverWebsocketClientTransportFactory(
                @Nullable ScheduledExecutorService scheduledExecutorService,
                @Nullable ExecutorService transportExecutorService
        ) {
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

            return new GrpcOverWebsocketClientTransport(
                    this.transportExecutorService,
                    maxInboundMetadataSize,
                    maxInboundMessageSize,
                    options,
                    endpointUri,
                    authority,
                    clientListener
            );
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
