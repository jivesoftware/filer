package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 */
public interface Copyable<V> {

    void copyTo(V to) throws IOException;
}
