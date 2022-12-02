package kr.jclab.grpcoverwebsocket.client;

import io.grpc.*;
import io.grpc.internal.*;
import kr.jclab.grpcoverwebsocket.client.internal.AbstractCgrpcClientTransport;
import kr.jclab.grpcoverwebsocket.portable.WritableSocket;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.framing.Framedata;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class GrpcOverWebsocketClientTransport
        extends AbstractCgrpcClientTransport<GrpcOverWebsocketClientTransport>
        implements WritableSocket
{
    private final URI endpointUri;
    private final WebSocketClient webSocketClient;

    public GrpcOverWebsocketClientTransport(
            ExecutorService transportExecutorService,
            ScheduledExecutorService scheduledExecutorService,
            int maxInboundMetadataSize,
            int maxInboundMessageSize,
            ClientTransportFactory.ClientTransportOptions options,
            URI endpointUri,
            String authority,
            WebsocketClientListener<GrpcOverWebsocketClientTransport> clientListener
    ) {
        super(
                transportExecutorService,
                scheduledExecutorService,
                maxInboundMetadataSize,
                maxInboundMessageSize,
                options,
                endpointUri.getScheme().startsWith("wss") ? SecurityLevel.NONE : SecurityLevel.PRIVACY_AND_INTEGRITY,
                authority,
                clientListener
        );
        this.endpointUri = endpointUri;

        this.webSocketClient = new WebSocketClient(
                this.endpointUri
        ) {
            @Override
            public void onOpen(ServerHandshake handshakedata) {
                GrpcOverWebsocketClientTransport.this.onOpen();
            }

            @Override
            public void onMessage(String message) {
                throw new RuntimeException("Never called");
            }

            @Override
            public void onMessage(ByteBuffer receiveBuffer) {
                GrpcOverWebsocketClientTransport.this.onMessage(receiveBuffer);
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                GrpcOverWebsocketClientTransport.this.onClose();
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
                GrpcOverWebsocketClientTransport.this.onError(ex);
            }

            @Override
            public void onWebsocketPing(WebSocket conn, Framedata f) {
                GrpcOverWebsocketClientTransport.this.onPing();
                super.onWebsocketPing(conn, f);
            }

            @Override
            public void onWebsocketPong(WebSocket conn, Framedata f) {
                GrpcOverWebsocketClientTransport.this.onPong();
                super.onWebsocketPong(conn, f);
            }
        };
        clientListener.onWebsocketCreated(this, this.webSocketClient);

        this.writableSocket = this;
    }

    @Override
    public void connect() {
        this.webSocketClient.connect();
    }

    @Override
    public void send(ByteBuffer byteBuffer) {
        if (this.webSocketClient.isOpen()) {
            this.webSocketClient.send(byteBuffer);
        }
    }

    @Override
    public void sendPing() {
        this.webSocketClient.sendPing();
    }

    @Override
    public void close() {
        this.webSocketClient.close();
    }
}
