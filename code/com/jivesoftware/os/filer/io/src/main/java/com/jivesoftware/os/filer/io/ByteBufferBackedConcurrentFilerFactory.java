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
    public <R> R reallocate(byte[] key, long newSize, FilerTransaction<ByteBufferBackedFiler, R> reallocateFiler) throws IOException {

        ByteBuffer bb = byteBufferFactory.allocate(key, newSize);
        ByteBufferBackedFiler newFiler = new ByteBufferBackedFiler(new Object(), bb);
        return reallocateFiler.commit(newFiler);
    }

}
