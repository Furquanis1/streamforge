package com.streamforge.common.codec;

import com.streamforge.proto.EncodingType;

public final class BatchCodecFactory {
    private BatchCodecFactory() {
    }

    public static BatchCodec get(EncodingType type) {
        if (type == null) {
            throw new IllegalArgumentException("EncodingType must not be null");
        }
        return switch (type) {
            case PLAIN_ROW -> new PlainRowCodec();
            case COLUMNAR_DELTA -> new ColumnarDeltaCodec();
            case UNRECOGNIZED -> throw new IllegalArgumentException(
                    "Unsupported encoding type: " + type);
        };
    }
}
