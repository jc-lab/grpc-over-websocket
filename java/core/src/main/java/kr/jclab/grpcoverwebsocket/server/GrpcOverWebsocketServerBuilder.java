package kr.jclab.grpcoverwebsocket.server;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ServerBuilder;
import io.grpc.ServerStreamTracer;
import io.grpc.inprocess.InProcessSocketAddress;
import io.grpc.internal.*;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class GrpcOverWebsocketServerBuilder extends AbstractServerImplBuilder<GrpcOverWebsocketServerBuilder> {
    protected final ServerImplBuilder serverImplBuilder;
    protected ScheduledExecutorService scheduledExecutorService;
    protected ExecutorService transportExecutorService;
    protected GrpcOverWebsocketTransceiver transceiver;
    protected GrpcOverWebsocketServerHandler serverHandler;
    protected TransportTracer.Factory transportTracerFactory = TransportTracer.getDefaultFactory();

    protected int maxInboundMetadataSize = GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE;
    protected int maxInboundMessageSize = GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE;

    public GrpcOverWebsocketServerBuilder() {
        this.serverImplBuilder = new ServerImplBuilder(new GrpcOverWebsocketClientTransportServersBuilder());
    }

    public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void setTransportExecutorService(ExecutorService transportExecutorService) {
        this.transportExecutorService = transportExecutorService;
    }

    public void setStatsEnabled(boolean value) {
        this.serverImplBuilder.setStatsEnabled(value);
    }

    public void attachTransceiver(GrpcOverWebsocketTransceiver transceiver) {
        this.transceiver = transceiver;
    }

    public void setServerHandler(GrpcOverWebsocketServerHandler serverHandler) {
        this.serverHandler = serverHandler;
    }

    public void setTransportTracerFactory(TransportTracer.Factory transportTracerFactory) {
        this.transportTracerFactory = transportTracerFactory;
    }

    @Override
    public GrpcOverWebsocketServerBuilder maxInboundMetadataSize(int bytes) {
        super.maxInboundMetadataSize(bytes);
        this.maxInboundMetadataSize = bytes;
        return this;
    }

    @Override
    public GrpcOverWebsocketServerBuilder maxInboundMessageSize(int bytes) {
        super.maxInboundMessageSize(bytes);
        this.maxInboundMessageSize = bytes;
        return this;
    }

    @Override
    protected ServerBuilder<?> delegate() {
        return this.serverImplBuilder;
    }

    @VisibleForTesting
    GrpcOverWebsocketServer buildTransportServers(List<? extends ServerStreamTracer.Factory> streamTracerFactories) {
        // this.scheduledExecutorService
        return new GrpcOverWebsocketServer(
                maxInboundMetadataSize,
                maxInboundMessageSize,
                streamTracerFactories,
                this.transportTracerFactory,
                this.scheduledExecutorService,
                this.transportExecutorService,
                this.transceiver,
                this.serverHandler,
                new InProcessSocketAddress("SERVER")
        );
    }

    final class GrpcOverWebsocketClientTransportServersBuilder implements ServerImplBuilder.ClientTransportServersBuilder {
        @Override
        public InternalServer buildClientTransportServers(List<? extends ServerStreamTracer.Factory> streamTracerFactories) {
            return buildTransportServers(streamTracerFactories);
        }
    }
}
