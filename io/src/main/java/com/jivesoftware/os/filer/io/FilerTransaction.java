package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 * @param <F>
 * @param <R>
 */
public interface FilerTransaction<F extends Filer, R> {

    R commit(Object lock, F filer) throws IOException;
}
