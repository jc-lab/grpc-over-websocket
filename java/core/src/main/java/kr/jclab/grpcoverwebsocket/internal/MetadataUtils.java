package kr.jclab.grpcoverwebsocket.internal;

import io.grpc.InternalMetadata;
import io.grpc.Metadata;

public class MetadataUtils {
    public static int metadataSize(byte[][] serialized) {
        if (serialized == null) {
            return 0;
        }
        // Calculate based on SETTINGS_MAX_HEADER_LIST_SIZE in RFC 7540 §6.5.2. We could use something
        // different, but it's "sane."
        long size = 0;
        for (int i = 0; i < serialized.length; i += 2) {
            size += 32 + serialized[i].length + serialized[i + 1].length;
        }
        size = Math.min(size, Integer.MAX_VALUE);
        return (int) size;
    }

    public static int metadataSize(Metadata metadata) {
        return metadataSize(InternalMetadata.serialize(metadata));
    }
}
