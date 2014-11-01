package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;

/**
 * @author jonathan.colt
 */
public class PrimitivesMapStoresBuilder {

    private MapChunkFactory mapChunkFactory;

    public PrimitivesMapStoresBuilder() {
    }

    public PrimitivesMapStoresBuilder setMapChunkFactory(MapChunkFactory mapChunkFactory) {
        this.mapChunkFactory = mapChunkFactory;
        return this;
    }

    public KeyValueStore<Long, Long> buildLongLong() {
        return new BytesBytesMapStore<>("8", 8, null, mapChunkFactory, new KeyValueMarshaller<Long, Long>() {

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
        return new BytesBytesMapStore<>("8", 8, null, mapChunkFactory, new KeyValueMarshaller<Long, Integer>() {

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
