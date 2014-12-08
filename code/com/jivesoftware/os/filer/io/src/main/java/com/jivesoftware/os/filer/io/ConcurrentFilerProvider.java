package com.jivesoftware.os.filer.io;

import java.io.IOException;

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

    public F allocate(long size) throws IOException {
        return factory.allocate(key, size);
    }

    public F reallocate(F old, long newSize) throws Exception {
        return factory.reallocate(key, old, newSize);
    }
}
