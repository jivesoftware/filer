package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;

/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
/**
 *
 * @author jonathan.colt
 */
public class PrimitivesMapStoresBuilder {

    private ByteBufferFactory bufferFactory = new HeapByteBufferFactory();
    private int initialPageCapacity = 8;

    public PrimitivesMapStoresBuilder() {
    }

    public PrimitivesMapStoresBuilder setByteBufferFactory(ByteBufferFactory bufferFactory) {
        this.bufferFactory = bufferFactory;
        return this;
    }

    public PrimitivesMapStoresBuilder setInitialPageCapacity(int initialPageCapacity) {
        this.initialPageCapacity = initialPageCapacity;
        return this;
    }

    public KeyValueStore<Long, Long> buildLongLong() {
        return new BytesBytesMapStore<>(8, false, 8, false, initialPageCapacity, null, bufferFactory, new KeyValueMarshaller<Long, Long>() {

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
        return new BytesBytesMapStore<>(8, false, 4, false, initialPageCapacity, null, bufferFactory, new KeyValueMarshaller<Long, Integer>() {

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
