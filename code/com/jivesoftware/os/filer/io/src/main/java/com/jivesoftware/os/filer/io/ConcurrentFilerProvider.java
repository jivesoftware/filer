package com.jivesoftware.os.filer.io;

import java.io.IOException;

import static com.jivesoftware.os.filer.io.ConcurrentFilerFactory.ReallocateFiler;

/**
 *
 * @author jonathan.colt
 * @param <F>
 */
public class ConcurrentFilerProvider<F extends ConcurrentFiler> {

    private final byte[] key;
    private final ConcurrentFilerFactory<F> factory;

    public ConcurrentFilerProvider(byte[] key, ConcurrentFilerFactory<F> factory) {
        this.key = key;
        this.factory = factory;
    }

    public F get() throws IOException {
        return factory.get(key);
    }

    public F allocate(long size) throws IOException {
        return factory.allocate(key, size);
    }

    public <R> R reallocate(long newSize, ReallocateFiler<F, R> reallocateFiler) throws Exception {
        return factory.reallocate(key, newSize, reallocateFiler);
    }
}
