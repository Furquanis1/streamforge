package com.streamforge.common.integrity;

import java.util.zip.CRC32;

public final class ChecksumUtil {
    private ChecksumUtil() {
    }

    public static long computeCrc32(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return crc32.getValue();
    }

    public static boolean verifyCrc32(byte[] data, long expectedChecksum) {
        return computeCrc32(data) == expectedChecksum;
    }

    public static void assertValidCrc32(byte[] data, long expectedChecksum) {
        long actualChecksum = computeCrc32(data);
        if (actualChecksum != expectedChecksum) {
            throw new ChecksumMismatchException(expectedChecksum, actualChecksum);
        }
    }
}
