package kr.jclab.grpcoverwebsocket.example;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import kr.jclab.grpcoverwebsocket.client.ClientListener;
import kr.jclab.grpcoverwebsocket.client.GrpcOverWebsocketChannelBuilder;
import kr.jclab.grpcoverwebsocket.client.GrpcOverWebsocketClientConnection;
import kr.jclab.grpcoverwebsocket.sample.model.*;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.Iterator;
import java.util.Observable;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class ClientApplication {
    public void run() throws Exception {
        GrpcOverWebsocketChannelBuilder builder = GrpcOverWebsocketChannelBuilder.forTarget("ws://127.0.0.1:9999/ws");
        builder.setClientListener(new ClientListener() {
        });
        ManagedChannel clientChannel = builder.build();

        log.info("GreeterGrpc.newStub BEFORE");
        GreeterGrpc.GreeterStub greeterGrpc = GreeterGrpc.newStub(clientChannel);
        log.info("GreeterGrpc.newStub AFTER");

        for (int i=0; i<2; i++) {
            final int currentLoop = i;
            Thread.sleep(1000);

            if (true) {
                log.info("[{}] greeterGrpc.sayHello BEFORE", currentLoop);
                CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                greeterGrpc.sayHello(
                        HelloRequest.newBuilder()
                                .setName("Apple")
                                .build(),
                        new StreamObserver<HelloReply>() {
                            @Override
                            public void onNext(HelloReply value) {
                                log.info("[{}] greeterGrpc.sayHello REPLY / " + value, currentLoop);
                            }

                            @Override
                            public void onError(Throwable t) {
                                completableFuture.completeExceptionally(t);
                            }

                            @Override
                            public void onCompleted() {
                                completableFuture.complete(null);
                            }
                        }
                );
                completableFuture.get();
                log.info("[{}] greeterGrpc.sayHello AFTER", currentLoop);
            }

            if (true) {
                log.info("[{}] greeterGrpc.muchHello BEFORE", currentLoop);
                CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                greeterGrpc.muchHello(
                        HelloRequest.newBuilder()
                                .setName("Banana")
                                .build(),
                        new StreamObserver<HelloReply>() {
                            @Override
                            public void onNext(HelloReply value) {
                                log.info("[{}] greeterGrpc.muchHello REPLY / " + value, currentLoop);
                            }

                            @Override
                            public void onError(Throwable t) {
                                completableFuture.completeExceptionally(t);
                            }

                            @Override
                            public void onCompleted() {
                                completableFuture.complete(null);
                            }
                        }
                );
                completableFuture.get();
                log.info("[{}] greeterGrpc.muchHello AFTER", currentLoop);
            }

            if (true) {
                log.info("[{}] greeterGrpc.talks BEFORE", currentLoop);
                CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                StreamObserver<MessageRequest> stream = greeterGrpc.talks(
                        new StreamObserver<MessageReply>() {
                            @Override
                            public void onNext(MessageReply value) {
                                log.info("[{}] greeterGrpc.talks REPLY / " + value, currentLoop);
                            }

                            @Override
                            public void onError(Throwable t) {
                                log.info("[{}] greeterGrpc.talks ERROR", currentLoop, t);
                                completableFuture.completeExceptionally(t);
                            }

                            @Override
                            public void onCompleted() {
                                log.info("[{}] greeterGrpc.talks COMPLETE", currentLoop);
                                completableFuture.complete(null);
                            }
                        }
                );
                stream.onNext(
                        MessageRequest.newBuilder()
                                .setMessage("Hello")
                                .build()
                );
                stream.onNext(
                        MessageRequest.newBuilder()
                                .setMessage("How are you today?")
                                .build()
                );
                stream.onCompleted();
                completableFuture.get();
                log.info("[{}] greeterGrpc.talks AFTER", currentLoop);
            }
        }

        log.info("clientChannel.shutdown BEFORE");
        clientChannel.shutdown();
        log.info("clientChannel.shutdown AFTER");
    }
}
