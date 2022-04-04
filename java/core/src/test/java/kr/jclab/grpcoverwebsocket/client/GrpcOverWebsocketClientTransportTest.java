package kr.jclab.grpcoverwebsocket.client;

import io.grpc.ServerStreamTracer;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.server.SimpleWebsocketServerBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.simple.SimpleLogger;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(JUnit4.class)
public class GrpcOverWebsocketClientTransportTest extends AbstractTransportTest {
    private final FakeClock fakeClock = new FakeClock();
    private boolean preventSamePort = true;
    private AtomicBoolean stopOnTerminated = new AtomicBoolean(false);

    @Before
    public void before() {
        System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug");
        this.preventSamePort = true;
        this.stopOnTerminated.set(false);
    }

    @Override
    protected InternalServer newServer(List<ServerStreamTracer.Factory> streamTracerFactories) {
        SimpleWebsocketServerBuilder builder = new SimpleWebsocketServerBuilder(38000 + new SecureRandom().nextInt(27000), stopOnTerminated);
        builder.setTransportTracerFactory(this.fakeClockTransportTracer);
        return builder.buildTransportServers(streamTracerFactories);
    }

    @Override
    protected InternalServer newServer(int port, List<ServerStreamTracer.Factory> streamTracerFactories) {
        SimpleWebsocketServerBuilder builder = new SimpleWebsocketServerBuilder(port + (preventSamePort ? 1 : 0), stopOnTerminated);
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

    @Override
    public void serverAlreadyListening() throws Exception {
        this.preventSamePort = false;
        super.serverAlreadyListening();
    }

    @Test
    public void earlyServerClose_serverFailure_withClientCancelOnListenerClosed() throws Exception {
        super.earlyServerClose_serverFailure_withClientCancelOnListenerClosed();
    }

    @Test
    public void socketStats() throws Exception {
        super.socketStats();
    }

    @Test
    public void serverCancel() throws Exception {
         super.serverCancel();
    }

    @Test
    public void openStreamPreventsTermination() throws Exception {
        super.openStreamPreventsTermination();
    }

    @Test
    public void shutdownNowKillsServerStream() throws Exception {
        super.shutdownNowKillsServerStream();
    }

    @Test
    public void checkClientAttributes() throws Exception {
        super.checkClientAttributes();
    }

    @Test
    public void serverStartInterrupted() throws Exception {
        super.serverStartInterrupted();
    }

    @Test
    public void transportInUse_balancerRpcsNotCounted() throws Exception {
        super.transportInUse_balancerRpcsNotCounted();
    }

    @Test
    public void transportInUse_clientCancel() throws Exception {
        super.transportInUse_clientCancel();
    }

    @Test
    public void flowControlPushBack() throws Exception {
        super.flowControlPushBack();
    }

    @Test
    public void serverNotListening() throws Exception {
        this.stopOnTerminated.set(true);
        super.serverNotListening();
    }
}
