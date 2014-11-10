package com.jivesoftware.os.filer.io;

/**
 *
 * @author jonathan.colt
 * @param <K>
 */
public class StripingLocksProvider<K> {

    private final Object[] locks;

    public StripingLocksProvider(int numLocks) {
        locks = new Object[numLocks];
        for (int i = 0; i < numLocks; i++) {
            locks[i] = new Object();
        }
    }

    public Object lock(K toLock) {
        return locks[Math.abs(toLock.hashCode() % locks.length)];
    }
}