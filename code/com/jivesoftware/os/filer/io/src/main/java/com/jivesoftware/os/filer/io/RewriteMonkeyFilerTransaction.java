package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 */
public interface RewriteMonkeyFilerTransaction<M, F extends Filer, R> {

    /**
     *
     * @param currentMonkey the current monkey, null if there is no existing data
     * @param currentFiler the current filer, null if there is no existing data
     * @param newMonkey the new monkey, for data to be written
     * @param newFiler the new filer, for data to be written
     * @return the typed result
     * @throws IOException
     */
    R commit(M currentMonkey, F currentFiler, M newMonkey, F newFiler) throws IOException;
}
