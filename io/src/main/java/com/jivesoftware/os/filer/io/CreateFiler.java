package com.jivesoftware.os.filer.io;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.io.IOException;

/**
 *
 * @param <H> Hint for creations
 * @param <M> Monkey type
 * @param <F> Filer type
 */
public interface CreateFiler<H, M, F extends Filer> {

    long sizeInBytes(H hint) throws IOException;

    M create(H hint, F filer, StackBuffer stackBuffer) throws IOException;

}
