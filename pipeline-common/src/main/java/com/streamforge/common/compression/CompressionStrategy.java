package com.streamforge.common.compression;

import com.streamforge.proto.CompressionType;

public interface CompressionStrategy {
    CompressionType getType();

    byte[] compress(byte[] rawData);

    byte[] decompress(byte[] compressedData, int originalUncompressedLength);
}
