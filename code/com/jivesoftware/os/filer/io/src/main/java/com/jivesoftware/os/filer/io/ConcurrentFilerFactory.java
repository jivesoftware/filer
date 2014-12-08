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
     * @param old can be null and if it is you could have just called allocate which is what implementation should do.
     * @param newSize
     * @return
     * @throws java.io.IOException
     */
    F reallocate(byte[] key, F old, long newSize) throws IOException;
}
