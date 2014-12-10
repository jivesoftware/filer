package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 */
public interface RewriteFilerTransaction<F extends Filer, R> {

    /**
     *
     * @param currentFiler the current filer, null if there is no existing data
     * @param newFiler the new filer, for data to be written
     * @return the typed result
     * @throws IOException
     */
    R commit(F currentFiler, F newFiler) throws IOException;
}
