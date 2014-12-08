package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ConcurrentFiler;
import java.io.IOException;

/**
 * @author jonathan
 * @param <F>
 */
public class MapChunk<F extends ConcurrentFiler> {

    final F filer;
    int keySize; // read only
    byte keyLengthSize; // read only
    int payloadSize; // read only
    byte payloadLengthSize; // read only
    int capacity; // read only
    int maxCount; // read only
    int entrySize; // read only

    /**
     * @param filer
     */
    public MapChunk(F filer) {
        this.filer = filer;
    }

    /**
     * TODO consider factor this out to call sights??
     *
     * @return
     * @throws IOException
     */
    public MapChunk duplicate() throws IOException {
        MapChunk mapChunk = new MapChunk(filer.asConcurrentReadWrite(filer.lock())); // hmm is filer.lock() the right thing to do?
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
     * @param mapStore
     */
    public void init(MapStore mapStore) throws IOException {
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
    public F raw() {
        return filer;
    }

    /**
     * @param pageStart
     * @return
     */
    byte read(long pageStart) throws IOException {
        filer.seek(pageStart);
        return (byte) filer.read();
    }

    /**
     * @param pageStart
     * @return
     */
    int readUnsingedByte(long pageStart) throws IOException {
        filer.seek(pageStart);
        return (byte) filer.read();
    }

    /**
     * @param pageStart
     * @param v
     */
    void write(long pageStart, byte v) throws IOException {
        filer.seek(pageStart);
        filer.write(v);
    }

    /**
     * @param pageStart
     * @return
     */
    int readShort(long pageStart) throws IOException {
        filer.seek(pageStart);
        byte[] bytes = new byte[2];
        filer.read(bytes);
        short v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    /**
     * @param pageStart
     * @return
     */
    int readUnsignedShort(long pageStart) throws IOException {
        filer.seek(pageStart);
        byte[] bytes = new byte[2];
        filer.read(bytes);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    /**
     * @param pageStart
     * @return
     */
    int readInt(long pageStart) throws IOException {
        filer.seek(pageStart);
        byte[] bytes = new byte[4];
        filer.read(bytes);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        return v;
    }

    /**
     * @param pageStart
     * @return
     */
    float readFloat(long pageStart) throws IOException {
        filer.seek(pageStart);
        byte[] bytes = new byte[4];
        filer.read(bytes);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        return Float.intBitsToFloat(v);
    }

    /**
     * @param pageStart
     * @return
     */
    long readLong(long pageStart) throws IOException {
        filer.seek(pageStart);
        byte[] bytes = new byte[8];
        filer.read(bytes);
        long v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        v <<= 8;
        v |= (bytes[4] & 0xFF);
        v <<= 8;
        v |= (bytes[5] & 0xFF);
        v <<= 8;
        v |= (bytes[6] & 0xFF);
        v <<= 8;
        v |= (bytes[7] & 0xFF);
        return v;
    }

    /**
     * @param pageStart
     * @return
     */
    double readDouble(long pageStart) throws IOException {
        filer.seek(pageStart);
        byte[] bytes = new byte[8];
        filer.read(bytes);
        long v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        v <<= 8;
        v |= (bytes[4] & 0xFF);
        v <<= 8;
        v |= (bytes[5] & 0xFF);
        v <<= 8;
        v |= (bytes[6] & 0xFF);
        v <<= 8;
        v |= (bytes[7] & 0xFF);
        return Double.longBitsToDouble(v);
    }

    void writeUnsignedShort(long pageStart, int v) throws IOException {
        filer.seek(pageStart);
        filer.write(new byte[]{
            (byte) (v >>> 8),
            (byte) v
        });
    }

    /**
     * @param pageStart
     * @param v
     */
    void writeInt(long pageStart, int v) throws IOException {
        filer.seek(pageStart);
        filer.write(new byte[]{
            (byte) (v >>> 24),
            (byte) (v >>> 16),
            (byte) (v >>> 8),
            (byte) v
        });
    }

    /**
     * @param pageStart
     * @param read
     * @param offset
     * @param length
     */
    void read(int pageStart, byte[] read, int offset, int length) throws IOException {
        filer.seek(pageStart);
        filer.read(read, offset, length);
    }

    /**
     * @param pageStart
     * @param towrite
     * @param offest
     * @param length
     */
    void write(int pageStart, byte[] towrite, int offest, int length) throws IOException {
        filer.seek(pageStart);
        filer.write(towrite, offest, length);
    }

}
