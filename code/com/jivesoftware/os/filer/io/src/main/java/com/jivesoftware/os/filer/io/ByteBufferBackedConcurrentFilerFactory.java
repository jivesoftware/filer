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
    public <M, R> R getOrAllocate(byte[] key,
        long size,
        OpenFiler<M, ByteBufferBackedFiler> openFiler,
        CreateFiler<M, ByteBufferBackedFiler> createFiler,
        MonkeyFilerTransaction<M, ByteBufferBackedFiler, R> filerTransaction)
        throws IOException {

        IBA iba = new IBA(key);
        ByteBufferBackedFiler filer = bufferCache.get(iba);
        if (filer != null) {
            return filerTransaction.commit(null, filer);
        } else if (size > 0) {
            return allocate(key, size, filerTransaction);
        } else {
            return filerTransaction.commit(null, null);
        }
    }

    private <M, R> R allocate(byte[] key, long size, MonkeyFilerTransaction<M, ByteBufferBackedFiler, R> filerTransaction) throws IOException {
        ByteBuffer bb = byteBufferFactory.allocate(key, size);
        ByteBufferBackedFiler filer = new ByteBufferBackedFiler(new Object(), bb);
        ByteBufferBackedFiler had = bufferCache.putIfAbsent(new IBA(key), filer);
        if (had != null) {
            return filerTransaction.commit(null, had);
        } else {
            return filerTransaction.commit(null, filer);
        }
    }

    @Override
    public <M, R> R grow(byte[] key,
        long newSize,
        OpenFiler<M, ByteBufferBackedFiler> openFiler,
        CreateFiler<M, ByteBufferBackedFiler> createFiler,
        RewriteMonkeyFilerTransaction<M, ByteBufferBackedFiler, R> filerTransaction)
        throws IOException {

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

        return filerTransaction.commit(null, oldFiler, null, filer);
    }

    @Override
    public void delete(byte[] key) throws IOException {
        bufferCache.remove(new IBA(key));
    }
}
