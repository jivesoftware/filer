package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;

/**
 * @author jonathan.colt
 */
public class PrimitivesMapStoresBuilder {

    private ByteBufferProvider byteBufferProvider;
    private int initialPageCapacity = 8;

    public PrimitivesMapStoresBuilder() {
    }

    public PrimitivesMapStoresBuilder setByteBufferProvider(ByteBufferProvider byteBufferProvider) {
        this.byteBufferProvider = byteBufferProvider;
        return this;
    }

    public PrimitivesMapStoresBuilder setInitialPageCapacity(int initialPageCapacity) {
        this.initialPageCapacity = initialPageCapacity;
        return this;
    }

    public KeyValueStore<Long, Long> buildLongLong() {
        return new BytesBytesMapStore<>(8, false, 8, false, initialPageCapacity, null, byteBufferProvider, new KeyValueMarshaller<Long, Long>() {

            @Override
            public byte[] keyBytes(Long key) {
                return FilerIO.longBytes(key);
            }

            @Override
            public byte[] valueBytes(Long value) {
                return FilerIO.longBytes(value);
            }

            @Override
            public Long bytesKey(byte[] bytes, int offset) {
                return FilerIO.bytesLong(bytes, offset);
            }

            @Override
            public Long bytesValue(Long key, byte[] bytes, int offset) {
                return FilerIO.bytesLong(bytes, offset);
            }
        });
    }

    public KeyValueStore<Long, Integer> buildLongInt() {
        return new BytesBytesMapStore<>(8, false, 4, false, initialPageCapacity, null, byteBufferProvider, new KeyValueMarshaller<Long, Integer>() {

            @Override
            public byte[] keyBytes(Long key) {
                return FilerIO.longBytes(key);
            }

            @Override
            public byte[] valueBytes(Integer value) {
                return FilerIO.intBytes(value);
            }

            @Override
            public Long bytesKey(byte[] bytes, int offset) {
                return FilerIO.bytesLong(bytes, offset);
            }

            @Override
            public Integer bytesValue(Long key, byte[] bytes, int offset) {
                return FilerIO.bytesInt(bytes, offset);
            }
        });
    }
}
