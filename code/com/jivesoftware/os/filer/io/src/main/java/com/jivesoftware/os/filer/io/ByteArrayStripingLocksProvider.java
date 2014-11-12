package com.jivesoftware.os.filer.io;

import java.util.Arrays;

/**
 *
 */
public class ByteArrayStripingLocksProvider {

    private final ByteArrayStripingLock[] locks;

    public ByteArrayStripingLocksProvider(int numLocks) {
        locks = new ByteArrayStripingLock[numLocks];
        for (int i = 0; i < numLocks; i++) {
            locks[i] = new ByteArrayStripingLock();
        }
    }

    public Object lock(byte[] toLock) {
        return locks[Math.abs(Arrays.hashCode(toLock) % locks.length)];
    }

    static private class ByteArrayStripingLock {
    }
}