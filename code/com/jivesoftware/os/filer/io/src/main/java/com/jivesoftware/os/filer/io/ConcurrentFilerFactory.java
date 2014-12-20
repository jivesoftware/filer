package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 * @param <F>
 * @author jonathan.colt
 */
public interface ConcurrentFilerFactory<F extends Filer> {

    /**
     *
     * @param key the filer key
     * @param size initial capacity for the filer if it does not already exist, negative if no filer should be created
     * @param openFiler to execute when opening existing filer
     * @param createFiler to execute when creating a new filer
     * @param filerTransaction the transaction to be executed, where the filer may be null if no filer exists and the initial size is negative
     * @param <M> the monkey type
     * @param <R> the result type
     * @return the result
     * @throws IOException
     */
    <M, R> R getOrAllocate(byte[] key,
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
     * @param <M> the monkey type
     * @param <R> the result type
     * @return the result
     * @throws IOException
     */
    <M, R> R grow(byte[] key,
        long newSize,
        OpenFiler<M, F> openFiler,
        CreateFiler<Long, M, F> createFiler,
        RewriteMonkeyFilerTransaction<M, F, R> filerTransaction) throws IOException;

    /**
     * @param key the filer key
     * @throws IOException
     */
    void delete(byte[] key) throws IOException;

}
