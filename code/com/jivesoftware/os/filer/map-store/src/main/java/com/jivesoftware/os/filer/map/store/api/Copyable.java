package com.jivesoftware.os.filer.map.store.api;

/**
 *
 */
public interface Copyable<V, E extends Exception> {

    void copyTo(V to) throws E;
}
