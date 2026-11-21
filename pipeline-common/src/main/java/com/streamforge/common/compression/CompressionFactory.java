package com.streamforge.common.compression;

import com.streamforge.proto.CompressionType;

import java.util.EnumMap;
import java.util.Map;

public final class CompressionFactory {
    private static final Map<CompressionType, CompressionStrategy> STRATEGIES;

    static {
        EnumMap<CompressionType, CompressionStrategy> strategies =
                new EnumMap<>(CompressionType.class);
        strategies.put(CompressionType.RAW_NONE, new NoOpCompression());
        strategies.put(CompressionType.GZIP, new GzipCompression());
        strategies.put(CompressionType.LZ4, new Lz4Compression());
        strategies.put(CompressionType.ZSTD, new ZstdCompression());
        STRATEGIES = java.util.Collections.unmodifiableMap(strategies);
    }

    private CompressionFactory() {
    }

    public static CompressionStrategy get(CompressionType type) {
        if (type == null) {
            throw new IllegalArgumentException("CompressionType must not be null");
        }
        CompressionStrategy strategy = STRATEGIES.get(type);
        if (strategy == null) {
            throw new IllegalArgumentException("Unsupported compression type: " + type);
        }
        return strategy;
    }
}
