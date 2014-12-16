package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 */
public interface CreateFiler<M, F extends Filer> {
    M create(F filer) throws IOException;
}
