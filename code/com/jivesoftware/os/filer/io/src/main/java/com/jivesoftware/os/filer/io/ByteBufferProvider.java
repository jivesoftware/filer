package com.jivesoftware.os.filer.io;

import java.nio.ByteBuffer;

/**
 *
 */
public class ByteBufferProvider {

    private final byte[] key;
    private final ByteBufferFactory byteBufferFactory;

    public ByteBufferProvider(byte[] key, ByteBufferFactory byteBufferFactory) {
        this.key = key;
        this.byteBufferFactory = byteBufferFactory;
    }

    public ByteBuffer allocate(long _size) {
        return byteBufferFactory.allocate(key, _size);
    }

    public ByteBuffer reallocate(ByteBuffer oldBuffer, long newSize) {
        return byteBufferFactory.reallocate(key, oldBuffer, newSize);
    }

}
