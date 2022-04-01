package kr.jclab.grpcoverwebsocket.client.command;

import io.grpc.Status;
import kr.jclab.grpcoverwebsocket.internal.OrderedQueue;
import lombok.Getter;

public class GracefulCloseCommand extends OrderedQueue.AbstractQueuedCommand {
    @Getter
    private final Status reason;

    public GracefulCloseCommand(Status reason) {
        this.reason = reason;
    }
}
