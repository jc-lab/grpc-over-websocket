package kr.jclab.grpcoverwebsocket.server.command;

import io.grpc.Status;
import kr.jclab.grpcoverwebsocket.internal.OrderedQueue;
import kr.jclab.grpcoverwebsocket.server.internal.ServerStreamImpl;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class CancelServerStreamCommand extends OrderedQueue.AbstractQueuedCommand {
    @Getter
    private final ServerStreamImpl stream;
    @Getter
    private final Status status;
}
