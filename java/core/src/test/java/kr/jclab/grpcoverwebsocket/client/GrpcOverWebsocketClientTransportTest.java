package kr.jclab.grpcoverwebsocket.client;

import io.grpc.ServerStreamTracer;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.server.SimpleWebsocketServerBuilder;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GrpcOverWebsocketClientTransportTest extends AbstractTransportTest {
    private final FakeClock fakeClock = new FakeClock();

    @Override
    protected InternalServer newServer(List<ServerStreamTracer.Factory> streamTracerFactories) {
        SimpleWebsocketServerBuilder builder = SimpleWebsocketServerBuilder.forPort(38000 + new SecureRandom().nextInt(27000));
        builder.setTransportTracerFactory(this.fakeClockTransportTracer);
        return builder.buildTransportServers(streamTracerFactories);
    }

    @Override
    protected InternalServer newServer(int port, List<ServerStreamTracer.Factory> streamTracerFactories) {
        SimpleWebsocketServerBuilder builder = SimpleWebsocketServerBuilder.forPort(port + 1);
        builder.setTransportTracerFactory(this.fakeClockTransportTracer);
        return builder.buildTransportServers(streamTracerFactories);
    }

    @Override
    protected void advanceClock(long offset, TimeUnit unit) {
        fakeClock.forwardNanos(unit.toNanos(offset));
    }

    @Override
    protected long fakeCurrentTimeNanos() {
        return fakeClock.getTicker().read();
    }

    @Override
    protected ManagedClientTransport newClientTransport(InternalServer server) {
        int port = ((InetSocketAddress) server.getListenSocketAddress()).getPort();
        GrpcOverWebsocketChannelBuilder builder = GrpcOverWebsocketChannelBuilder.forTarget("ws://localhost:" + port);
        return builder.buildTransportFactory().newClientTransport(
                new InetSocketAddress("localhost", port),
                new ClientTransportFactory.ClientTransportOptions()
                        .setAuthority(testAuthority(server))
                        .setEagAttributes(eagAttrs()),
                transportLogger()
        );
    }

    @Override
    protected String testAuthority(InternalServer server) {
        return "";
//        return "thebestauthority:" + server.getListenSocketAddress();
    }

    @Test
    public void serverStartInterrupted() throws Exception {
        super.serverStartInterrupted();
    }

    @Override
    public void shutdownNowKillsClientStream() throws Exception {
        super.shutdownNowKillsClientStream();
    }

    @Override
    public void ping_afterTermination() throws Exception {
        super.ping_afterTermination();
    }

    @Override
    public void serverNotListening() throws Exception {
        super.serverNotListening();
    }

    @Override
    public void transportInUse_balancerRpcsNotCounted() throws Exception {
        super.transportInUse_balancerRpcsNotCounted();
    }

    @Override
    public void clientStartAndStopOnceConnected() throws Exception {
        super.clientStartAndStopOnceConnected();
    }
}
