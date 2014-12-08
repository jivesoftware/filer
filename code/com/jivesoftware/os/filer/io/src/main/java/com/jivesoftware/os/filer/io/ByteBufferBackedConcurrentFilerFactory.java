package com.jivesoftware.os.filer.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public class ByteBufferBackedConcurrentFilerFactory implements ConcurrentFilerFactory<ByteBufferBackedFiler> {

    private final ByteBufferFactory byteBufferFactory;

    public ByteBufferBackedConcurrentFilerFactory(ByteBufferFactory byteBufferFactory) {
        this.byteBufferFactory = byteBufferFactory;
    }

    @Override
    public ByteBufferBackedFiler get(byte[] key) throws IOException {
        return null;
    }

    @Override
    public ByteBufferBackedFiler allocate(byte[] key, long size) {
        ByteBuffer bb = byteBufferFactory.allocate(key, size);
        return new ByteBufferBackedFiler(new Object(), bb);
    }

    @Override
    public ByteBufferBackedFiler reallocate(byte[] key, ByteBufferBackedFiler old, long newSize) throws IOException {
        ByteBuffer bb = byteBufferFactory.allocate(key, newSize);
        ByteBufferBackedFiler newFiler = new ByteBufferBackedFiler(new Object(), bb);
        if (old != null) {
            FilerIO.copy(old, newFiler, -1); // Kinda lame wish we could do a bytebuffer copy :(
        }
        return newFiler;
    }

}
