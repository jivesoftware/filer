package com.jivesoftware.os.filer.io;

/**
 *
 */
public class NoOpCreateFiler<F extends Filer> implements CreateFiler<Void, F> {

    public NoOpCreateFiler() {
    }

    @Override
    public Void create(F filer) {
        return null;
    }
}
