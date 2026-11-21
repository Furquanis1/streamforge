package com.streamforge.common.compression;

import com.streamforge.proto.CompressionType;

import java.util.Arrays;

public final class NoOpCompression implements CompressionStrategy {
    @Override
    public CompressionType getType() {
        return CompressionType.RAW_NONE;
    }

    @Override
    public byte[] compress(byte[] rawData) {
        return Arrays.copyOf(rawData, rawData.length);
    }

    @Override
    public byte[] decompress(byte[] compressedData, int originalUncompressedLength) {
        if (originalUncompressedLength < 0 || originalUncompressedLength > compressedData.length) {
            throw new IllegalArgumentException("Invalid uncompressed length");
        }
        return Arrays.copyOf(compressedData, originalUncompressedLength);
    }
}
