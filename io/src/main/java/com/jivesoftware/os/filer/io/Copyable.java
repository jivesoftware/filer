package com.jivesoftware.os.filer.io;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.io.IOException;

/**
 *
 * @param <V>
 */
public interface Copyable<V> {

    void copyTo(V to, StackBuffer stackBuffer) throws IOException, InterruptedException;
}
