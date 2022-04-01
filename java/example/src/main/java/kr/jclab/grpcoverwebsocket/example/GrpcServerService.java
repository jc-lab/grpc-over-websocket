package kr.jclab.grpcoverwebsocket.example;

import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import kr.jclab.grpcoverwebsocket.sample.model.*;
import kr.jclab.grpcoverwebsocket.server.GrpcOverWebsocketServerHandler;
import kr.jclab.grpcoverwebsocket.server.GrpcOverWebsocketServerBuilder;
import kr.jclab.grpcoverwebsocket.server.GrpcOverWebsocketTransceiver;
import kr.jclab.grpcoverwebsocket.server.GrpcWebSocketSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

@Component
@Slf4j
public class GrpcServerService extends BinaryWebSocketHandler {
    private final GrpcOverWebsocketTransceiver transceiver = new GrpcOverWebsocketTransceiver();

    private Server server;

    @PostConstruct
    protected void start() throws IOException {
        GrpcOverWebsocketServerBuilder builder = new GrpcOverWebsocketServerBuilder();
        builder.addService(new GreeterGrpc.GreeterImplBase() {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                log.warn("sayHello Invoked (name = {})", request.getName());
                responseObserver.onNext(
                        HelloReply.newBuilder()
                                .setMessage("HELLO WORLD!")
                                .build()
                );
                responseObserver.onCompleted();
            }

            @Override
            public void muchHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                log.warn("muchHello Invoked (name = {})", request.getName());
                responseObserver.onNext(
                        HelloReply.newBuilder()
                                .setMessage("안녕하세요!")
                                .build()
                );
                responseObserver.onNext(
                        HelloReply.newBuilder()
                                .setMessage("Hello!")
                                .build()
                );
                responseObserver.onNext(
                        HelloReply.newBuilder()
                                .setMessage("Bonjour!")
                                .build()
                );
                responseObserver.onCompleted();
            }

            @Override
            public StreamObserver<MessageRequest> talks(StreamObserver<MessageReply> responseObserver) {
                return new StreamObserver<MessageRequest>() {
                    @Override
                    public void onNext(MessageRequest value) {
                        responseObserver.onNext(
                                MessageReply.newBuilder()
                                        .setMessage(value.getMessage())
                                        .build()
                        );
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(t);
                    }

                    @Override
                    public void onCompleted() {
                        responseObserver.onCompleted();
                    }
                };
            }
        });
        builder.attachTransceiver(this.transceiver);
        builder.setServerHandler(new GrpcOverWebsocketServerHandler() {
            // IMPLEMENT IT IF NEEDED
        });
        this.server = builder.build();
        this.server.start();
    }

    @PreDestroy
    protected void stop() {
        if (this.server != null) {
            this.server.shutdown();
            this.server = null;
        }
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        log.info("WS[{}] afterConnectionEstablished", session);
        this.transceiver.afterConnectionEstablished(new WrappedWebSocketSession(session));
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.info("WS[{}] handleTransportError", session, exception);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        log.info("WS[{}] afterConnectionClosed: {}", session, status);
        this.transceiver.afterConnectionClosed(new WrappedWebSocketSession(session));
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {
        log.info("WS[{}] handleBinaryMessage {} bytes", session, message.getPayloadLength());
        this.transceiver.handleMessage(new WrappedWebSocketSession(session), message.getPayload());
    }

    public static class WrappedWebSocketSession implements GrpcWebSocketSession {
        private final WebSocketSession session;

        public WrappedWebSocketSession(WebSocketSession session) {
            this.session = session;
        }

        @Override
        public String getId() {
            return this.session.getId();
        }

        @Override
        public void sendMessage(ByteBuffer buffer) throws IOException {
            session.sendMessage(new BinaryMessage(buffer));
        }

        @Override
        public void close() {
            try {
                session.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public String getAuthority() {
            return "";
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return session.getRemoteAddress();
        }

        @Override
        public SocketAddress getLocalAddress() {
            return session.getLocalAddress();
        }
    }
}
