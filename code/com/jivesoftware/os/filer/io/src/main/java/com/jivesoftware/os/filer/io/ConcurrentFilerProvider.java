package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 * @param <F>
 */
public class ConcurrentFilerProvider<F extends Filer> {

    private final byte[] key;
    private final ConcurrentFilerFactory<F> factory;

    public ConcurrentFilerProvider(byte[] key, ConcurrentFilerFactory<F> factory) {
        this.key = key;
        this.factory = factory;
    }

    public <H, M, R> R getOrAllocate(long size, OpenFiler<M, F> openFiler, CreateFiler<Long, M, F> createFiler,
        MonkeyFilerTransaction<M, F, R> filerTransaction)
        throws IOException {
        return factory.getOrAllocate(key, size, openFiler, createFiler, filerTransaction);
    }

    public <H, M, R> R grow(long newSize, OpenFiler<M, F> openFiler, CreateFiler<Long, M, F> createFiler,
        RewriteMonkeyFilerTransaction<M, F, R> filerTransaction)
        throws IOException {
        return factory.grow(key, newSize, openFiler, createFiler, filerTransaction);
    }

    public void delete() throws IOException {
        factory.delete(key);
    }
}
