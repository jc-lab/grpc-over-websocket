package kr.jclab.grpcoverwebsocket.client.internal;

import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.grpc.Status;
import io.grpc.internal.WritableBuffer;

import javax.annotation.Nullable;
import java.util.List;

public interface FrameWriter {
    void newStream(int streamId, List<ByteString> serializedMetadata, String fullMethodName, @Nullable byte[] payload);
    void data(int streamId, WritableBuffer frame, boolean endOfStream);
    void closeStream(int streamId, Code code);
    void goAway(Status.Code code);
    void close();
    void ping();
}
