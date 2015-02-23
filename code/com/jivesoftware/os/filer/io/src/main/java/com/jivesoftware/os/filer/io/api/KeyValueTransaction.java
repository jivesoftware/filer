package com.jivesoftware.os.filer.io.api;

import java.io.IOException;

/**
 *
 */
public interface KeyValueTransaction<V, R> {
    R commit(KeyValueContext<V> context) throws IOException;
}
