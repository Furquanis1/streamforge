package com.streamforge.common.integrity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChecksumUtilTest {
    @Test
    void validCrc32PassesAndCorruptionThrowsChecksumMismatch() {
        byte[] data = "StreamForge integrity test".getBytes();
        long checksum = ChecksumUtil.computeCrc32(data);

        assertTrue(ChecksumUtil.verifyCrc32(data, checksum));
        assertDoesNotThrow(() -> ChecksumUtil.assertValidCrc32(data, checksum));

        byte[] corrupted = data.clone();
        corrupted[0] ^= 0x01;
        assertThrows(ChecksumMismatchException.class,
                () -> ChecksumUtil.assertValidCrc32(corrupted, checksum));
    }
}
