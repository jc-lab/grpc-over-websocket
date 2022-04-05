package kr.jclab.grpcoverwebsocket.protocol.v1;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.InternalMetadata;
import io.grpc.Metadata;
import io.grpc.Status;
import kr.jclab.grpcoverwebsocket.core.protocol.v1.*;
import kr.jclab.grpcoverwebsocket.internal.HandshakeState;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class ProtocolHelper {
    public static <TContext> void handleMessage(ProtocolHandler<TContext> handler, TContext context, ByteBuffer receiveBuffer) throws InvalidProtocolBufferException, BufferUnderflowException {
        byte payloadTypeValue = receiveBuffer.get();
        PayloadType payloadType = PayloadType.fromValue(payloadTypeValue);

        checkNotNull(payloadType, "Invalid payload type: " + payloadTypeValue);

        switch (payloadType) {
            case HANDSHAKE:
                checkState(HandshakeState.HANDSHAKE.equals(handler.getHandshakeState()));
                handler.handleHandshakeMessage(context, receiveBuffer);
                break;

            case CONTROL:
                byte controlTypeValue = receiveBuffer.get();
                ControlType controlType = ControlType.fromValue(controlTypeValue);
                checkNotNull(controlType, "Invalid control type: " + controlTypeValue);

                if (ControlType.HandshakeResult.equals(controlType)) {
                    checkState(HandshakeState.HANDSHAKE.equals(handler.getHandshakeState()));
                    handleHandshakeResult(handler, context, receiveBuffer);
                } else {
                    checkState(HandshakeState.COMPLETE.equals(handler.getHandshakeState()));
                    handleControl(handler, context, controlType, receiveBuffer);
                }

                break;

            case GRPC:
                checkState(HandshakeState.COMPLETE.equals(handler.getHandshakeState()));
                handleGrpcMessage(handler, context, receiveBuffer);
                break;
        }
    }

    static <TContext>  void handleGrpcMessage(ProtocolHandler<TContext> handler, TContext context, ByteBuffer receiveBuffer) throws BufferUnderflowException {
        byte flagsValue = receiveBuffer.get();
        int streamId = readInt32LE(receiveBuffer);
        EnumSet<GrpcStreamFlag> flags = (flagsValue == 0) ? EnumSet.noneOf(GrpcStreamFlag.class) : EnumSet.copyOf(
                Arrays.stream(GrpcStreamFlag.values())
                        .filter(v -> (v.getValue() & flagsValue) != 0)
                        .collect(Collectors.toList())
        );
        handler.handleGrpcStream(context, streamId, flags, receiveBuffer);
    }

    static <TContext>  void handleHandshakeResult(ProtocolHandler<TContext> handler, TContext context, ByteBuffer receiveBuffer) throws InvalidProtocolBufferException {
        HandshakeResult handshakeResult = HandshakeResult.newBuilder()
                .mergeFrom(ByteString.copyFrom(receiveBuffer))
                .build();
        handler.handleHandshakeResult(context, handshakeResult);
    }

    static <TContext>  void handleControl(ProtocolHandler<TContext> handler, TContext context, ControlType controlType, ByteBuffer receiveBuffer) throws InvalidProtocolBufferException {
        ByteString payload = ByteString.copyFrom(receiveBuffer);
        switch (controlType) {
            case NewStream:
                handler.handleNewStream(
                        context,
                        NewStream.newBuilder()
                                .mergeFrom(payload)
                                .build()
                );
                break;
            case StreamHeader:
                handler.handleStreamHeader(
                        context,
                        StreamHeader.newBuilder()
                                .mergeFrom(payload)
                                .build()
                );
                break;
            case CloseStream:
                handler.handleCloseStream(
                        context,
                        CloseStream.newBuilder()
                                .mergeFrom(payload)
                                .build()
                );
                break;
            case FinishTransport:
                handler.handleFinishTransport(
                        context,
                        FinishTransport.newBuilder()
                                .mergeFrom(payload)
                                .build()
                );
                break;
        }
    }

    public static ByteBuffer serializeHandshakeMessage(ByteBuffer buffer) {
        ByteBuffer sendBuffer = ByteBuffer.allocate(1 + buffer.remaining())
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(PayloadType.HANDSHAKE.getValue())
                .put(buffer);
        sendBuffer.flip();
        return sendBuffer;
    }

    public static ByteBuffer serializeControlMessage(ControlType controlType, GeneratedMessageV3 message) {
        byte[] data = message.toByteArray();
        ByteBuffer sendBuffer = ByteBuffer.allocate(2 + data.length)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(PayloadType.CONTROL.getValue())
                .put(controlType.getValue())
                .put(data);
        sendBuffer.flip();
        return sendBuffer;
    }

    public static List<ByteString> metadataSerialize(Metadata metadata) {
        if (metadata == null) return Collections.emptyList();
        return Arrays.stream(InternalMetadata.serialize(metadata))
                .map(ByteString::copyFrom)
                .collect(Collectors.toList());
    }

    public static Metadata metadataDeserialize(List<ByteString> payload) {
        byte[][] serializedMetadata = payload.stream()
                .map(ByteString::toByteArray)
                .toArray(byte[][]::new);
        return InternalMetadata.newMetadata(serializedMetadata);
    }

    public static int readInt32LE(ByteBuffer buffer) {
        if (ByteOrder.LITTLE_ENDIAN.equals(buffer.order())) {
            return buffer.getInt();
        } else {
            return swapEndian(buffer.getInt());
        }
    }

    public static int swapEndian(int i) {
        return ((i<<24) + ((i<<8)&0x00FF0000) + ((i>>8)&0x0000FF00) + (i>>>24));
    }

    public static Status statusFromProto(com.google.rpc.Status input) {
        Status status = Status.fromCodeValue(input.getCode());
        String message = input.getMessage();
        if (!message.isEmpty()) {
            status = status.withDescription(message);
        }
        return status;
    }

    public static com.google.rpc.Status statusToProto(Status input) {
        return com.google.rpc.Status.newBuilder()
                .setCode(
                        input.getCode().value()
                )
                .setMessage(
                        Optional.ofNullable(input.getDescription())
                                .orElse("")
                )
                .build();
    }
}
