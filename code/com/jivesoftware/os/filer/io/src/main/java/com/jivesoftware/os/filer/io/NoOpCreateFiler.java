package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 * @param <F>
 */
public class NoOpCreateFiler<F extends Filer> implements CreateFiler<Long, FilerLock, F> {


    public NoOpCreateFiler() {
    }

    @Override
    public FilerLock create(Long hint, F filer) {
        return new FilerLock();
    }

    @Override
    public long sizeInBytes(Long hint) throws IOException {
        return hint;
    }

}
