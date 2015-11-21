package com.jivesoftware.os.filer.io;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.io.IOException;

/**
 *
 * @param <F>
 */
public class NoOpCreateFiler<F extends Filer> implements CreateFiler<Long, Void, F> {

    public NoOpCreateFiler() {
    }

    @Override
    public Void create(Long hint, F filer, StackBuffer stackBuffer) {
        return null;
    }

    @Override
    public long sizeInBytes(Long hint) throws IOException {
        return hint;
    }

}
