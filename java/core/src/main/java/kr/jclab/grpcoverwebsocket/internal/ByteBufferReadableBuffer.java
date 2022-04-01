package kr.jclab.grpcoverwebsocket.internal;

import com.google.common.base.Preconditions;
import io.grpc.internal.AbstractReadableBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class ByteBufferReadableBuffer extends AbstractReadableBuffer {
    private final ByteBuffer bytes;

    public ByteBufferReadableBuffer(ByteBuffer bytes) {
        this.bytes = Preconditions.checkNotNull(bytes, "bytes");
    }

    @Override
    public int readableBytes() {
        return bytes.remaining();
    }

    @Override
    public int readUnsignedByte() {
        checkReadable(1);
        return bytes.get() & 0xFF;
    }

    @Override
    public void skipBytes(int length) {
        checkReadable(length);
        ((Buffer) bytes).position(bytes.position() + length);
    }

    @Override
    public void readBytes(byte[] dest, int destOffset, int length) {
        checkReadable(length);
        bytes.get(dest, destOffset, length);
    }

    @Override
    public void readBytes(ByteBuffer dest) {
        Preconditions.checkNotNull(dest, "dest");
        int length = dest.remaining();
        checkReadable(length);

        // Change the limit so that only length bytes are available.
        int prevLimit = bytes.limit();
        ((Buffer) bytes).limit(bytes.position() + length);

        // Write the bytes and restore the original limit.
        dest.put(bytes);
        bytes.limit(prevLimit);
    }

    @Override
    public void readBytes(OutputStream dest, int length) throws IOException {
        checkReadable(length);
        if (hasArray()) {
            dest.write(array(), arrayOffset(), length);
            ((Buffer) bytes).position(bytes.position() + length);
        } else {
            // The buffer doesn't support array(). Copy the data to an intermediate buffer.
            byte[] array = new byte[length];
            bytes.get(array);
            dest.write(array);
        }
    }

    @Override
    public ByteBufferReadableBuffer readBytes(int length) {
        checkReadable(length);
        ByteBuffer buffer = bytes.duplicate();
        ((Buffer) buffer).limit(bytes.position() + length);
        ((Buffer) bytes).position(bytes.position() + length);
        return new ByteBufferReadableBuffer(buffer);
    }

    @Override
    public boolean hasArray() {
        return bytes.hasArray();
    }

    @Override
    public byte[] array() {
        return bytes.array();
    }

    @Override
    public int arrayOffset() {
        return bytes.arrayOffset() + bytes.position();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark() {
        bytes.mark();
    }

    @Override
    public void reset() {
        bytes.reset();
    }

    @Override
    public boolean byteBufferSupported() {
        return true;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return bytes.slice();
    }
}
