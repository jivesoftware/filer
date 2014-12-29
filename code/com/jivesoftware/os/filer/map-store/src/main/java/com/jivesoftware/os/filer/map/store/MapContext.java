package com.jivesoftware.os.filer.map.store;

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

    public MapContext(int keySize, byte keyLengthSize, int payloadSize, byte payloadLengthSize, int capacity, int maxCount, int entrySize, long count) {
        this.keySize = keySize;
        this.keyLengthSize = keyLengthSize;
        this.payloadSize = payloadSize;
        this.payloadLengthSize = payloadLengthSize;
        this.capacity = capacity;
        this.maxCount = maxCount;
        this.entrySize = entrySize;
        this.count = count;
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
