package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 * @param <F> the filer type
 * @param <M> the monkey type
 * @author jonathan.colt
 */
public interface MagicMonkeyFactory<F extends Filer, P, M, Z> {

    /**
     *
     * @param key the filer key
     * @param size initial capacity for the filer if it does not already exist, negative if no filer should be created
     * @param openFiler to execute when opening existing filer
     * @param createFiler to execute when creating a new filer
     * @param filerTransaction the transaction to be executed, where the filer may be null if no filer exists and the initial size is negative
     * @param <R> the result type
     * @return the result
     * @throws java.io.IOException
     */
    <R> R getOrAllocate(final byte[] key,
        long size,
        OpenFiler<M, F> openFiler,
        CreateFiler<Long, M, F> createFiler,
        MonkeyFilerTransaction<M, F, R> filerTransaction)
        throws IOException;

    /**
     * @param key the filer key
     * @param newSize the new size
     * @param openFiler to execute when opening existing filer
     * @param createFiler to execute when creating a new filer
     * @param filerTransaction the transaction
     * @param <R> the result type
     * @return the result
     * @throws java.io.IOException
     */
    <R> R grow(final byte[] key,
        final long newSize,
        final OpenFiler<M, F> openFiler,
        final CreateFiler<Long, M, F> createFiler,
        final RewriteMonkeyFilerTransaction<M, F, R> filerTransaction) throws IOException;

    /**
     * @param key the filer key
     * @throws java.io.IOException
     */
    void delete(byte[] key) throws IOException;

}
