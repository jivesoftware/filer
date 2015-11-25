package com.jivesoftware.os.filer.io;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.io.IOException;

/**
 *
 * @param <M>
 * @param <F>
 */
public interface OpenFiler<M, F extends Filer> {

    M open(F filer, StackBuffer stackBuffer) throws IOException, InterruptedException;
}
