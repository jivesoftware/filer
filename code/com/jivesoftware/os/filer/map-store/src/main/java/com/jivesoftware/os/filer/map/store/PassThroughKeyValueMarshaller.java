package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.KeyValueMarshaller;

/**
 *
 */
public class PassThroughKeyValueMarshaller implements KeyValueMarshaller<byte[], byte[]> {

    public static final PassThroughKeyValueMarshaller INSTANCE = new PassThroughKeyValueMarshaller();

    private PassThroughKeyValueMarshaller() {
    }

    @Override
    public byte[] valueBytes(byte[] value) {
        return value;
    }

    @Override
    public byte[] bytesValue(byte[] key, byte[] value, int valueOffset) {
        return value;
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
