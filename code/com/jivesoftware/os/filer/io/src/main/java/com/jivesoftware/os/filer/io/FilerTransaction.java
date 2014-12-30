package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 * @param <F>
 */
public interface FilerTransaction<F extends Filer, R> {

    R commit(F filer) throws IOException;
}
