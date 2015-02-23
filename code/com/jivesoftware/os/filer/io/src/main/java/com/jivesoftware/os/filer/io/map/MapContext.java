package com.jivesoftware.os.filer.io.map;

/**
 * @author jonathan
 */
public class MapContext {

    public final int keySize; // read only
    public final byte keyLengthSize; // read only
    public final int payloadSize; // read only
    public final byte payloadLengthSize; // read only
    public final int capacity; // read only
    public final int maxCount; // read only
    public final int entrySize; // read only
    transient long count;
    transient long requested;

    public MapContext(int keySize, byte keyLengthSize, int payloadSize, byte payloadLengthSize, int capacity, int maxCount, int entrySize, long count) {
        this.keySize = keySize;
        this.keyLengthSize = keyLengthSize;
        this.payloadSize = payloadSize;
        this.payloadLengthSize = payloadLengthSize;
        this.capacity = capacity;
        this.maxCount = maxCount;
        this.entrySize = entrySize;
        this.count = count;
        this.requested = 0;
        if (keySize == 0 || capacity == 0 || maxCount == 0 || entrySize == 0) {
            System.out.println("Bad state for " + toString());
            Thread.dumpStack();
            throw new RuntimeException("Bad state for " + toString());
        }
    }

    @Override
    public String toString() {
        return "MapContext{"
            + "keySize=" + keySize
            + ", keyLengthSize=" + keyLengthSize
            + ", payloadSize=" + payloadSize
            + ", payloadLengthSize=" + payloadLengthSize
            + ", capacity=" + capacity
            + ", maxCount=" + maxCount
            + ", entrySize=" + entrySize
            + ", count=" + count + '}';
    }

}
