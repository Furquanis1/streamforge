package com.streamforge.common.compression;

import com.streamforge.proto.CompressionType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class GzipCompression implements CompressionStrategy {
    @Override
    public CompressionType getType() {
        return CompressionType.GZIP;
    }

    @Override
    public byte[] compress(byte[] rawData) {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(output)) {
            gzip.write(rawData);
            gzip.finish();
            gzip.flush();
            return output.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("GZIP compression failed", e);
        }
    }

    @Override
    public byte[] decompress(byte[] compressedData, int originalUncompressedLength) {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressedData))) {
            ByteArrayOutputStream output = new ByteArrayOutputStream(
                    Math.max(originalUncompressedLength, 0));
            byte[] buffer = new byte[8192];
            int read;
            while ((read = gzip.read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("GZIP decompression failed", e);
        }
    }
}
