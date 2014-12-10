package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 * @param <F>
 */
public interface ConcurrentFilerFactory<F extends ConcurrentFiler> {

    /**
     * @param key
     * @return
     * @throws java.io.IOException
     */
    F get(byte[] key) throws IOException;

    /**
     * @param key
     * @param size
     * @return
     * @throws java.io.IOException
     */
    F allocate(byte[] key, long size) throws IOException;

    /**
     * @param key
     * @param newSize
     * @return
     * @throws java.io.IOException
     */
    <R> R reallocate(byte[] key, long newSize, FilerTransaction<F, R> reallocateFilerTransaction) throws IOException;

}
