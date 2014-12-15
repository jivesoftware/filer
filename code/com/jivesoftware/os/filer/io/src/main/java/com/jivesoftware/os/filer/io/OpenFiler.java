package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 */
public interface OpenFiler<M, F extends Filer> {
    M open(F filer) throws IOException;
}
