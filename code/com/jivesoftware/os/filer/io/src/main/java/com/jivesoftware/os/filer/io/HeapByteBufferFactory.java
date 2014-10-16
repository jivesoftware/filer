package com.jivesoftware.os.filer.io;

import java.nio.ByteBuffer;

/**
 *
 */
public class HeapByteBufferFactory implements ByteBufferFactory {
    
    @Override
    public ByteBuffer allocate(long _size) {
        return ByteBuffer.allocate((int) _size);
    }
}