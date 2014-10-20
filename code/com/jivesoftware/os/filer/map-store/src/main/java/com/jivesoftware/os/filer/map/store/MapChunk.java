package com.jivesoftware.os.filer.map.store;

import java.nio.ByteBuffer;

/**
 *
 * @author jonathan
 */
public class MapChunk {

    final ByteBuffer array;
    int keySize; // read only
    byte keyLengthSize;// read only
    int payloadSize; // read only
    byte payloadLengthSize;// read only
    int capacity; // read only
    int maxCount; // read only
    int entrySize; // read only

    /**
     *
     * @param array
     */
    public MapChunk(ByteBuffer array) {
        this.array = array;
    }

    public MapChunk duplicate() {
        MapChunk mapChunk = new MapChunk(array.duplicate());
        mapChunk.keySize = keySize;
        mapChunk.keyLengthSize = keyLengthSize;
        mapChunk.payloadSize = payloadSize;
        mapChunk.payloadLengthSize = payloadLengthSize;
        mapChunk.capacity = capacity;
        mapChunk.maxCount = maxCount;
        mapChunk.entrySize = entrySize;
        return mapChunk;
    }

    /**
     *
     * @param mapStore
     */
    public void init(MapStore mapStore) {
        keySize = mapStore.getKeySize(this);
        keyLengthSize = mapStore.getKeyLengthSize(this);
        payloadSize = mapStore.getPayloadSize(this);
        payloadLengthSize = mapStore.getPayloadLengthSize(this);
        maxCount = mapStore.getMaxCount(this);
        capacity = mapStore.getCapacity(this);
        entrySize = keyLengthSize + keySize + payloadLengthSize + payloadSize;
    }

    /**
     * @return
     */
    public Object raw() {
        return array;
    }

    /**
     * @return
     */
    public long size() {
        return array.capacity();
    }

}
