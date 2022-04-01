package kr.jclab.grpcoverwebsocket.server;

import io.grpc.ServerStreamTracer;
import lombok.Getter;
import lombok.Setter;
import org.java_websocket.WebSocket;
import org.java_websocket.framing.Framedata;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class SimpleWebsocketServerBuilder extends GrpcOverWebsocketServerBuilder {
    private final GrpcOverWebsocketTransceiver transceiver = new GrpcOverWebsocketTransceiver();
    private final MyWebSocketServer myWebSocketServer;
    private final InetSocketAddress listenAddress;
    @Setter
    @Getter
    private String fakeAuthority = "";

    public SimpleWebsocketServerBuilder(int port) {
        super();
        this.listenAddress = new InetSocketAddress("127.0.0.1", port);
        this.myWebSocketServer = new MyWebSocketServer(port);

        this.attachTransceiver(transceiver);
        this.setServerHandler(new GrpcOverWebsocketServerHandler() {
            @Override
            public void onStart(GrpcOverWebsocketServer server) {
                myWebSocketServer.start();
            }

            @Override
            public void onTerminated(GrpcOverWebsocketServer server) {
//                try {
//                    myWebSocketServer.stop();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        });
    }

    public static SimpleWebsocketServerBuilder forPort(int port) {
        return new SimpleWebsocketServerBuilder(port);
    }

    private class MyWebSocketServer extends WebSocketServer {
        private CountDownLatch serverLatch = null;

        public MyWebSocketServer(int port, CountDownLatch serverLatch) {
            super(new InetSocketAddress(port));
            this.serverLatch = serverLatch;
        }

        public MyWebSocketServer(int port) {
            this(port, null);
        }

        @Override
        public void onOpen(WebSocket conn, ClientHandshake handshake) {
            GrpcWebSocketSessionImpl session = new GrpcWebSocketSessionImpl(conn, fakeAuthority);
            conn.setAttachment(session);
            try {
                transceiver.afterConnectionEstablished(session);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onClose(WebSocket conn, int code, String reason, boolean remote) {
            GrpcWebSocketSessionImpl session = conn.getAttachment();
            try {
                transceiver.afterConnectionClosed(session);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onMessage(WebSocket conn, String message) {
            throw new RuntimeException("never call");
        }

        @Override
        public void onMessage(WebSocket conn, ByteBuffer message) {
            GrpcWebSocketSessionImpl session = conn.getAttachment();
            try {
                transceiver.handleMessage(session, message);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onError(WebSocket conn, Exception ex) {
            ex.printStackTrace();
        }

        @Override
        public void onStart() {
            if (serverLatch != null) {
                serverLatch.countDown();
            }
        }

        @Override
        public void onWebsocketPing(WebSocket conn, Framedata f) {
            super.onWebsocketPing(conn, f);
        }

        @Override
        public void onWebsocketPong(WebSocket conn, Framedata f) {
            super.onWebsocketPong(conn, f);
        }
    }

    public static class GrpcWebSocketSessionImpl implements GrpcWebSocketSession {
        private final String id = UUID.randomUUID().toString();
        private final String authority;
        private final WebSocket conn;

        public GrpcWebSocketSessionImpl(WebSocket conn, String authority) {
            this.conn = conn;
            this.authority = authority;
        }

        @Override
        public String getId() {
            return this.id;
        }

        @Override
        public void sendMessage(ByteBuffer buffer) throws IOException {
            this.conn.send(buffer);
        }

        @Override
        public void close() {
            this.conn.close();
        }

        @Override
        public String getAuthority() {
            return this.authority;
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return this.conn.getRemoteSocketAddress();
        }

        @Override
        public SocketAddress getLocalAddress() {
            return this.conn.getLocalSocketAddress();
        }
    }

    @Override
    public GrpcOverWebsocketServer buildTransportServers(List<? extends ServerStreamTracer.Factory> streamTracerFactories) {
        return new GrpcOverWebsocketServer(
                maxInboundMetadataSize,
                maxInboundMessageSize,
                streamTracerFactories,
                this.transportTracerFactory,
                this.scheduledExecutorService,
                this.transportExecutorService,
                this.transceiver,
                this.serverHandler,
                this.listenAddress
        );
    }
}
