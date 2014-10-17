package com.jivesoftware.os.filer.map.store;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 *
 * @author jonathan
 */
public class MapChunk {

    private final ByteBuffer array;
    int keySize; // read only
    int payloadSize; // read only
    int capacity; // read only
    int maxCount; // read only

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
        mapChunk.payloadSize = payloadSize;
        mapChunk.capacity = capacity;
        mapChunk.maxCount = maxCount;
        return mapChunk;
    }

    /**
     *
     * @param mapStore
     */
    public void init(MapStore mapStore) {
        keySize = mapStore.getKeySize(this);
        payloadSize = mapStore.getPayloadSize(this);
        maxCount = mapStore.getMaxCount(this);
        capacity = mapStore.getCapacity(this);
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

    /**
     * @param pageStart
     * @return
     */
    public byte read(int pageStart) {
        return array.get(pageStart);
    }

    /**
     * @param pageStart
     * @param v
     */
    public void write(int pageStart, byte v) {
        array.put(pageStart, v);
    }

    /**
     * @param pageStart
     * @return
     */
    public int readInt(int pageStart) {
        return array.getInt(pageStart);
    }

    /**
     * @param pageStart
     * @return
     */
    public float readFloat(int pageStart) {
        return array.getFloat(pageStart);
    }

    /**
     * @param pageStart
     * @return
     */
    public long readLong(int pageStart) {
        return array.getLong(pageStart);
    }

    /**
     * @param pageStart
     * @return
     */
    public double readDouble(int pageStart) {
        return array.getDouble(pageStart);
    }

    /**
     * @param pageStart
     * @param v
     */
    public void writeInt(int pageStart, int v) {
        array.putInt(pageStart, v);
    }

    /**
     * @param pageStart
     * @param read
     * @param offset
     * @param length
     */
    public void read(int pageStart, byte[] read, int offset, int length) {
        array.position(pageStart);
        array.get(read, offset, length);
    }

    /**
     * @param pageStart
     * @param towrite
     * @param offest
     * @param length
     */
    public void write(int pageStart, byte[] towrite, int offest, int length) {
        array.position(pageStart);
        array.put(towrite, offest, length);
    }

    /**
     * @param pageStart
     * @param keySize
     * @param b
     * @param boffset
     * @return
     */
    public boolean equals(long pageStart, int keySize, byte[] b, int boffset) {
        for (int i = 0; i < keySize; i++) {
            int pageIndex = (int) (pageStart + 1 + i);
            if (array.get(pageIndex) != b[boffset + i]) {
                return false;
            }
        }
        return true;
    }

    /**
     *
     * @return
     */
    public boolean isLoaded() {
        if (array instanceof MappedByteBuffer) {
            return ((MappedByteBuffer) array).isLoaded();
        }
        return true;
    }

    /**
     *
     */
    public void force() {
        if (array instanceof MappedByteBuffer) {
            ((MappedByteBuffer) array).force();
        }
    }
}
