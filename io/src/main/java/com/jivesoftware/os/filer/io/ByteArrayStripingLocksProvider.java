package com.jivesoftware.os.filer.io;

import java.util.Arrays;

/**
 *
 */
public class ByteArrayStripingLocksProvider implements LocksProvider<byte[]> {

    private final ByteArrayStripingLock[] locks;

    public ByteArrayStripingLocksProvider(int numLocks) {
        locks = new ByteArrayStripingLock[numLocks];
        for (int i = 0; i < numLocks; i++) {
            locks[i] = new ByteArrayStripingLock();
        }
    }

    @Override
    public Object lock(byte[] toLock, int seed) {
        return locks[Math.abs((Arrays.hashCode(toLock) ^ seed) % locks.length)];
    }

    static private class ByteArrayStripingLock {
    }
}
