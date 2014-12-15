package com.jivesoftware.os.filer.map.store.api;

import java.io.IOException;

/**
 *
 */
public interface KeyValueContext<V> {

    void set(V value) throws IOException;

    void remove() throws IOException;

    V get() throws IOException;
}
