package kr.jclab.grpcoverwebsocket.portable;

public interface ClientSocket extends ReadableSocket {
    void onOpen();
    void onClose();
    void onError(Exception e);
}
