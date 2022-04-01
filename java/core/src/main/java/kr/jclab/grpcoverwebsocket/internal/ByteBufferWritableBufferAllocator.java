package kr.jclab.grpcoverwebsocket.internal;

import io.grpc.internal.WritableBuffer;
import io.grpc.internal.WritableBufferAllocator;

import java.nio.ByteBuffer;

public class ByteBufferWritableBufferAllocator implements WritableBufferAllocator {
    // Set the maximum buffer size to 1MB
    public static final int MAX_BUFFER = 1024 * 1024;

    @Override
    public WritableBuffer allocate(int capacityHint) {
        capacityHint = Math.min(MAX_BUFFER, capacityHint);
        return new ByteBufferWritableBuffer(ByteBuffer.allocateDirect(capacityHint), capacityHint);
    }
}
