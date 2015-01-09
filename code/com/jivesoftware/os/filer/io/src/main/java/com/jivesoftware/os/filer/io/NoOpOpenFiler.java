package com.jivesoftware.os.filer.io;

/**
 *
 * @param <F>
 */
public class NoOpOpenFiler<F extends Filer> implements OpenFiler<FilerLock, F> {

    @Override
    public FilerLock open(F filer) {
        return new FilerLock();
    }
}
