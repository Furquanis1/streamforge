package com.streamforge.common.compression;

import com.github.luben.zstd.Zstd;
import com.streamforge.proto.CompressionType;

public final class ZstdCompression implements CompressionStrategy {
    private static final int COMPRESSION_LEVEL = 3;

    @Override
    public CompressionType getType() {
        return CompressionType.ZSTD;
    }

    @Override
    public byte[] compress(byte[] rawData) {
        return Zstd.compress(rawData, COMPRESSION_LEVEL);
    }

    @Override
    public byte[] decompress(byte[] compressedData, int originalUncompressedLength) {
        return Zstd.decompress(compressedData, originalUncompressedLength);
    }
}
