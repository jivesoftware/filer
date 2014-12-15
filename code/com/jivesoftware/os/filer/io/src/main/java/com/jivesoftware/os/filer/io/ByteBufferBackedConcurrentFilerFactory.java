package com.jivesoftware.os.filer.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author jonathan.colt
 */
public class ByteBufferBackedConcurrentFilerFactory implements ConcurrentFilerFactory<ByteBufferBackedFiler> {

    private final ByteBufferFactory byteBufferFactory;
    private final ConcurrentHashMap<IBA, ByteBufferBackedFiler> bufferCache = new ConcurrentHashMap<>();

    public ByteBufferBackedConcurrentFilerFactory(ByteBufferFactory byteBufferFactory) {
        this.byteBufferFactory = byteBufferFactory;
    }

    @Override
    public <R> R getOrAllocate(byte[] key, long initialCapacity, final MonkeyFilerTransaction<ByteBufferBackedFiler, R> filerTransaction) throws IOException {
        IBA iba = new IBA(key);
        ByteBufferBackedFiler filer = bufferCache.get(iba);
        if (filer != null) {
            return filerTransaction.commit(filer, false);
        } else if (initialCapacity > 0) {
            return allocate(key, initialCapacity, filerTransaction);
        } else {
            return filerTransaction.commit(null, false);
        }
    }

    private <R> R allocate(byte[] key, long size, MonkeyFilerTransaction<ByteBufferBackedFiler, R> filerTransaction) throws IOException {
        ByteBuffer bb = byteBufferFactory.allocate(key, size);
        ByteBufferBackedFiler filer = new ByteBufferBackedFiler(new Object(), bb);
        ByteBufferBackedFiler had = bufferCache.putIfAbsent(new IBA(key), filer);
        if (had != null) {
            return filerTransaction.commit(had, false);
        } else {
            return filerTransaction.commit(filer, true);
        }
    }

    @Override
    public <R> R grow(byte[] key, long newSize, RewriteMonkeyFilerTransaction<ByteBufferBackedFiler, R> filerTransaction) throws IOException {
        IBA iba = new IBA(key);
        ByteBufferBackedFiler oldFiler = bufferCache.get(iba);
        ByteBufferBackedFiler filer = oldFiler;

        while (filer.length() < newSize) {
            ByteBuffer bb = byteBufferFactory.allocate(key, newSize);
            ByteBufferBackedFiler newFiler = new ByteBufferBackedFiler(new Object(), bb);
            if (bufferCache.replace(iba, filer, newFiler)) {
                filer = newFiler;
                break;
            } else {
                filer = bufferCache.get(iba);
            }
        }

        return filerTransaction.commit(oldFiler, filer);
    }

    @Override
    public void delete(byte[] key) throws IOException {
        bufferCache.remove(new IBA(key));
    }
}
