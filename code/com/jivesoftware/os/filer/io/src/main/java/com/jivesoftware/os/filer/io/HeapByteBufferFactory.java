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
    
    @Override
    public ByteBuffer reallocate(ByteBuffer oldBuffer, long newSize) {
        ByteBuffer newBuffer = allocate(newSize);
        if (oldBuffer != null) {
            oldBuffer.position(0);
            newBuffer.put(oldBuffer); // this assume we only grow. Blame Kevin :)
            newBuffer.position(0);
        }
        return newBuffer;
    }
}