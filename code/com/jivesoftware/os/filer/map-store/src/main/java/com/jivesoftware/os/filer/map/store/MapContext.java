package com.jivesoftware.os.filer.map.store;

/**
 * @author jonathan
 */
public class MapContext {

    final int keySize; // read only
    final byte keyLengthSize; // read only
    final int payloadSize; // read only
    final byte payloadLengthSize; // read only
    final int capacity; // read only
    final int maxCount; // read only
    final int entrySize; // read only
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

}
