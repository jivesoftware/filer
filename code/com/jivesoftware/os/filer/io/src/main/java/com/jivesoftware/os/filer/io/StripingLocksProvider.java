package com.jivesoftware.os.filer.io;

/**
 *
 * @author jonathan.colt
 * @param <K>
 */
public class StripingLocksProvider<K> implements LocksProvider<K> {

    private final StripingLock[] locks;

    public StripingLocksProvider(int numLocks) {
        locks = new StripingLock[numLocks];
        for (int i = 0; i < numLocks; i++) {
            locks[i] = new StripingLock();
        }
    }

    @Override
    public Object lock(K toLock, int seed) {
        return locks[Math.abs((toLock.hashCode() ^ seed) % locks.length)];
    }

    static private class StripingLock {
    }
}