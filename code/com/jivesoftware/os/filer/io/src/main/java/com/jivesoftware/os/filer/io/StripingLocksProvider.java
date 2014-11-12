package com.jivesoftware.os.filer.io;

/**
 *
 * @author jonathan.colt
 * @param <K>
 */
public class StripingLocksProvider<K> {

    private final StripingLock[] locks;

    public StripingLocksProvider(int numLocks) {
        locks = new StripingLock[numLocks];
        for (int i = 0; i < numLocks; i++) {
            locks[i] = new StripingLock();
        }
    }

    public Object lock(K toLock) {
        return locks[Math.abs(toLock.hashCode() % locks.length)];
    }

    static private class StripingLock {
    }
}