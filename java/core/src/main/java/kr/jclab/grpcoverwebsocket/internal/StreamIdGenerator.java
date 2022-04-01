package kr.jclab.grpcoverwebsocket.internal;

import java.util.concurrent.atomic.AtomicInteger;

public class StreamIdGenerator {
    public enum Mode {
        Server,
        Client
    }

    private final Mode mode;
    private final AtomicInteger streamId = new AtomicInteger(0);

    public StreamIdGenerator(Mode mode) {
        this.mode = mode;
        if (Mode.Client.equals(this.mode)) {
            this.streamId.set(1);
        }
    }

    public int getConnectionControlId() {
        return 0;
    }

    public int getUpgradeRequestId() {
        return 1;
    }

    public int nextId() throws StreamIdOverflowException {
        int id = this.streamId.getAndAdd(2);
        if (id < 0) {
            throw new StreamIdOverflowException();
        }
        return id;
    }
}
