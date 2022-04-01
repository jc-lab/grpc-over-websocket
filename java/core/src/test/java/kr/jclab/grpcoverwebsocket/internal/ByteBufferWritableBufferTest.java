package kr.jclab.grpcoverwebsocket.internal;

import io.grpc.internal.WritableBuffer;
import io.grpc.internal.WritableBufferTestBase;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.ByteBuffer;

@RunWith(JUnit4.class)
public class ByteBufferWritableBufferTest extends WritableBufferTestBase {
    private ByteBufferWritableBuffer buffer;

    @Before
    public void setup() {
        buffer = new ByteBufferWritableBuffer(ByteBuffer.allocate(100), 100);
    }

    @Override
    protected WritableBuffer buffer() {
        return buffer;
    }

    @Override
    protected byte[] writtenBytes() {
        ByteBuffer byteBuffer = buffer.buffer();
        byte[] writtenBytes = new byte[byteBuffer.position()];
        System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset(), writtenBytes, 0, writtenBytes.length);
        return writtenBytes;
    }
}
