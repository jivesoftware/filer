package com.jivesoftware.os.filer.io;

import com.jivesoftware.os.filer.io.api.StackBuffer;

/**
 *
 * @param <F>
 */
public class NoOpOpenFiler<F extends Filer> implements OpenFiler<Void, F> {

    @Override
    public Void open(F filer, StackBuffer stackBuffer) {
        return null;
    }
}
