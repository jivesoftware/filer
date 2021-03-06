package com.jivesoftware.os.filer.io;

import java.util.Arrays;

/**
 * Immutable Byte Array. Mainly intended to properly hash a byte array for use as a key/lock.
 */
public class IBA {

    private byte[] bytes;

    public IBA(byte[] bytes) {
        this.bytes = bytes;
    }

    public void violateImmutability(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IBA iba = (IBA) o;

        return Arrays.equals(bytes, iba.bytes);

    }

    @Override
    public int hashCode() {
        return bytes != null ? Arrays.hashCode(bytes) : 0;
    }

    @Override
    public String toString() {
        return "IBA{"
            + bytesToString()
            + '}';
    }

    private String bytesToString() {
        if (bytes == null) {
            return "null";
        } else if (bytes.length == 4) {
            return String.valueOf(FilerIO.bytesInt(bytes));
        } else if (bytes.length == 8) {
            return String.valueOf(FilerIO.bytesLong(bytes));
        } else {
            return "bytes=" + Arrays.toString(bytes);
        }
    }
}
