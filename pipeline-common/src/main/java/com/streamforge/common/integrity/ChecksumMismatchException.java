package com.streamforge.common.integrity;

public class ChecksumMismatchException extends RuntimeException {
    public ChecksumMismatchException(long expected, long actual) {
        super("CRC32 mismatch: expected=" + expected + ", actual=" + actual);
    }

    public ChecksumMismatchException(String message) {
        super(message);
    }
}
