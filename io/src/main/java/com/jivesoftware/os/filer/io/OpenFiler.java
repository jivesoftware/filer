package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 * @param <M>
 * @param <F>
 */
public interface OpenFiler<M, F extends Filer> {

    M open(F filer, byte[] primitiveBuffer) throws IOException;
}
