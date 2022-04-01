package kr.jclab.grpcoverwebsocket.protocol.v1;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum PayloadType {
    CONTROL((byte) 0x01),
    HANDSHAKE((byte) 0x02),
    GRPC((byte) 0x7F);

    private final byte value;

    PayloadType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    private static final Map<Byte, PayloadType> VALUES_MAP = Arrays.stream(PayloadType.values())
            .collect(Collectors.toMap(PayloadType::getValue, v -> v));
    public static PayloadType fromValue(byte value) {
        return VALUES_MAP.get(value);
    }
}
