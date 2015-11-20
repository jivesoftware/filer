package com.jivesoftware.os.filer.io;

/**
 *
 * @param <F>
 */
public class NoOpOpenFiler<F extends Filer> implements OpenFiler<Void, F> {

    @Override
    public Void open(F filer, byte[] primitiveBuffer) {
        return null;
    }
}
