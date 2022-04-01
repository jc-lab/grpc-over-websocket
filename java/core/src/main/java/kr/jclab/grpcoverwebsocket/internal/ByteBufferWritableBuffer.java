package kr.jclab.grpcoverwebsocket.internal;

import com.google.common.base.Preconditions;
import io.grpc.internal.WritableBuffer;

import java.nio.ByteBuffer;

public class ByteBufferWritableBuffer implements WritableBuffer {
    private final ByteBuffer buffer;

    public ByteBufferWritableBuffer(ByteBuffer buffer, int capacity) {
        this.buffer = Preconditions.checkNotNull(buffer, "buffer");
    }

    @Override
    public void write(byte[] src, int srcIndex, int length) {
        buffer.put(src, srcIndex, length);
    }

    @Override
    public void write(byte b) {
        buffer.put(b);
    }

    @Override
    public int writableBytes() {
        return buffer.remaining();
    }

    @Override
    public int readableBytes() {
        return buffer.position();
    }

    @Override
    public void release() {
    }

    public ByteBuffer buffer() {
        return buffer;
    }
}
