package kr.jclab.grpcoverwebsocket.protocol.v1;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum ControlType {
    HandshakeResult((byte) 0x01),
    NewStream((byte) 0x20),
    StreamHeader((byte) 0x21),
    CloseStream((byte) 0x2f),
    FinishTransport((byte) 0x71)
    ;

    private final byte value;

    ControlType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    private static final Map<Byte, ControlType> VALUES_MAP = Arrays.stream(ControlType.values())
            .collect(Collectors.toMap(ControlType::getValue, v -> v));
    public static ControlType fromValue(byte value) {
        return VALUES_MAP.get(value);
    }
}
