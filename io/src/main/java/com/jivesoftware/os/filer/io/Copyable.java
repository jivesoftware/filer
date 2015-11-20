package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 * @param <V>
 */
public interface Copyable<V> {

    void copyTo(V to, byte[] buffer) throws IOException;
}
