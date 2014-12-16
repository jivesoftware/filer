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
            return filerTransaction.commit(openFiler.open(filer), filer);
        } else if (size > 0) {
            ByteBuffer bb = byteBufferFactory.allocate(key, size);
            filer = new ByteBufferBackedFiler(new Object(), bb);
            ByteBufferBackedFiler had = bufferCache.putIfAbsent(new IBA(key), filer);
            if (had != null) {
                return filerTransaction.commit(createFiler.create(had), had);
            } else {
                return filerTransaction.commit(createFiler.create(filer), filer);
            }
        } else {
            return filerTransaction.commit(null, null);
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

        return filerTransaction.commit(openFiler.open(oldFiler), oldFiler, createFiler.create(filer), filer);
    }

    @Override
    public void delete(byte[] key) throws IOException {
        bufferCache.remove(new IBA(key));
    }

}
