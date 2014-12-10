package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 */
public interface FilerTransaction<F extends Filer, R> {

    R commit(F filer) throws IOException;
}
