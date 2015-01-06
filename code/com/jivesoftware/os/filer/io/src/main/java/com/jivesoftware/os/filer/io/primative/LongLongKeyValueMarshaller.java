package com.jivesoftware.os.filer.io.primative;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;

/**
 *
 */
public class LongLongKeyValueMarshaller implements KeyValueMarshaller<Long, Long> {

    @Override
    public byte[] keyBytes(Long key) {
        return FilerIO.longBytes(key);
    }

    @Override
    public Long bytesKey(byte[] keyBytes, int offset) {
        return FilerIO.bytesLong(keyBytes, offset);
    }

    @Override
    public Long bytesValue(Long key, byte[] valueBytes, int offset) {
        return FilerIO.bytesLong(valueBytes, offset);
    }

    @Override
    public byte[] valueBytes(Long value) {
        return FilerIO.longBytes(value);
    }

}
