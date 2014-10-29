package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.KeyMarshaller;

/**
 *
 */
public class PassThroughKeyMarshaller implements KeyMarshaller<byte[]> {

    public static final PassThroughKeyMarshaller INSTANCE = new PassThroughKeyMarshaller();

    private PassThroughKeyMarshaller() {
    }

    @Override
    public byte[] keyBytes(byte[] key) {
        return key;
    }

    @Override
    public byte[] bytesKey(byte[] keyBytes, int offset) {
        return keyBytes;
    }
}
