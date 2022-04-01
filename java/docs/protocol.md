# Websocket Payload

```
Common Header
 [ 1 Byte ] PAYLOAD_TYPE
Data
 ...
```

# PAYLOAD_TYPE

## 0x01 : Control Data

## 0x02 : Handshake Data

User Defined Data

## 0x7F : gRPC Stream

# Control Data

```
[ 1 Byte  ] PAYLOAD_TYPE (0x01)
[ 1 Byte  ] CONTROL_TYPE
[ n Bytes ] ProtoBuf Payload
```

## CONTROL_TYPE

### 0x01 : HandshakeResult

### 0x20 : NewStream

### 0x21 : StreamHeader

### 0x2f : CloseStream

### 0x71 : FinishTransport

## Handshake Data

Fully User Defined Data

## gRPC Stream

```
[ 1 Byte  ] PAYLOAD_TYPE (0x7F)
[ 1 Byte  ] FLAG
[ 4 Bytes ] STREAM_ID
[ n Bytes ] DATA
```

### FLAG

* 0x01 : EOF
