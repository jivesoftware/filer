package com.jivesoftware.os.filer.io;

/**
 *
 */
public interface Copyable<V, E extends Exception> {

    void copyTo(V to) throws E;
}
