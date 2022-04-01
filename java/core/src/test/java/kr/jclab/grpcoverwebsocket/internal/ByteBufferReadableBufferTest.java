package kr.jclab.grpcoverwebsocket.internal;

import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.ReadableBufferTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@RunWith(JUnit4.class)
public class ByteBufferReadableBufferTest extends ReadableBufferTestBase {
    private ByteBufferReadableBuffer buffer;

    /** Initialize buffer. */
    @SuppressWarnings("resource")
    @Before
    public void setup() {
        byte[] msgBinary = msg.getBytes(StandardCharsets.UTF_8);
        ByteBuffer tmp = ByteBuffer.allocate(msgBinary.length);
        tmp.put(msgBinary); tmp.flip();

        buffer = new ByteBufferReadableBuffer(tmp);
    }

    @Override
    @Test
    public void readToByteBufferShouldSucceed() {
        // Not supported.
    }

    @Override
    @Test
    public void partialReadToByteBufferShouldSucceed() {
        // Not supported.
    }

    @Override
    @Test
    public void markAndResetWithReadShouldSucceed() {
        // Not supported.
    }

    @Override
    @Test
    public void markAndResetWithReadToReadableBufferShouldSucceed() {
        // Not supported.
    }

    @Override
    protected ReadableBuffer buffer() {
        return buffer;
    }
}
