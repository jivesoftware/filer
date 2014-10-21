package com.jivesoftware.os.filer.map.store.extractors;

/**
 * @param <E>
 * @author jonathan.colt
 */
public interface IndexStream<E extends Throwable> {

    /**
     * @param i will be -1 as a end of stream marker.
     * @return false to stop stream.
     * @throws E
     */
    boolean stream(long i) throws E;

}
