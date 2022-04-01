package kr.jclab.grpcoverwebsocket.protocol.v1;

public enum GrpcStreamFlag {
    EndOfFrame((byte) 0x01)
    ;

    private final byte value;

    GrpcStreamFlag(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public boolean isContainFrom(byte target) {
        return (target & value) != 0;
    }
}
