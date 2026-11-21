package com.streamforge.common.compression;

import com.streamforge.proto.CompressionType;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public final class Lz4Compression implements CompressionStrategy {
    private final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    private final LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

    @Override
    public CompressionType getType() {
        return CompressionType.LZ4;
    }

    @Override
    public byte[] compress(byte[] rawData) {
        int maxLength = compressor.maxCompressedLength(rawData.length);
        byte[] compressed = new byte[maxLength];
        int length = compressor.compress(rawData, 0, rawData.length, compressed, 0, maxLength);
        byte[] result = new byte[length];
        System.arraycopy(compressed, 0, result, 0, length);
        return result;
    }

    @Override
    public byte[] decompress(byte[] compressedData, int originalUncompressedLength) {
        byte[] restored = new byte[originalUncompressedLength];
        decompressor.decompress(compressedData, 0, restored, 0, originalUncompressedLength);
        return restored;
    }
}
