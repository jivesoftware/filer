package com.jivesoftware.os.filer.map.store;

//!! POORLY TESTING
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.map.store.extractors.IndexStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

/**
 * this is a key+payload set that is backed buy a byte array. It is a fixed size set. It will not grow or shrink. You need to be aware and expect that your
 * system will cause the set to throw OverCapacityExceptions. The goal is to create a collection which will page to and from disk or net as fast as possible.
 * Nothing is synchronized to make it thread safe you need to synchronize higher up.
 *
 *
 * @author jonathan
 */
public class MapStore {

    public static final MapStore DEFAULT = new MapStore();

    public static final byte cVariableSized = 1;
    public static final byte cPageFamily = 1;
    public static final byte cPageVersion = 1;
    private static final int cPageVersionSize = 1;
    private static final int cPageFamilySize = 1;
    private static final int cIdSize = 16;
    private static final int cVersion = 8;
    private static final int cCountSize = 4;
    private static final int cMaxCountSize = 4;
    private static final int cMaxCapacitySize = 4;
    private static final int cKeySizeSize = 4;
    private static final int cPayloadSize = 4;

    private static final int cHeaderSize = cPageFamilySize + cPageVersionSize + cIdSize + cVersion + cCountSize
            + cMaxCountSize + cMaxCapacitySize + cVariableSized + cKeySizeSize + cVariableSized + cPayloadSize;

    private static final int cPageFamilyOffset = 0;
    private static final int cPageVersionOffset = cPageFamilySize;
    private static final int cIdOffset = cPageVersionOffset + cPageVersionSize;
    private static final int cVersionOffset = cIdOffset + cIdSize;
    private static final int cCountOffset = cVersionOffset + cVersion;
    private static final int cMaxCountOffset = cCountOffset + cCountSize;
    private static final int cCapacityOffset = cMaxCountOffset + cMaxCountSize;
    private static final int cKeySizeOffset = cCapacityOffset + cMaxCapacitySize;
    private static final int cKeySizeVariableOffset = cKeySizeOffset + cKeySizeSize;
    private static final int cPayloadSizeOffset = cKeySizeVariableOffset + cVariableSized;
    private static final int cPayloadSizeVariableOffset = cPayloadSizeOffset + cPayloadSize;

    private static final double cSetDensity = 0.6d;
    private static final byte cSkip = -1;
    private static final byte cNull = 0;

    private MapStore() {
    }

    /**
     *
     * @param _maxKeys
     * @param _keySize
     * @param _payloadSize
     * @return
     */
    int cost(int _maxKeys, int _keySize, int _payloadSize) {
        int maxCapacity = (int) (_maxKeys + (_maxKeys - (_maxKeys * cSetDensity)));
        // 1+ for head of entry status byte. 0 and -1 reserved
        int entrySize = 1 + _keySize + _payloadSize;
        return cHeaderSize + (entrySize * maxCapacity);
    }

    /**
     *
     * @param _keySize
     * @param _payloadSize
     * @return
     */
    public long absoluteMaxCount(int _keySize, int _payloadSize) {
        // 1+ for head of entry status byte. 0 and -1 reserved
        int entrySize = 1 + _keySize + _payloadSize;
        long maxCount = (Integer.MAX_VALUE - cHeaderSize) / entrySize;
        return (long) (maxCount * cSetDensity);
    }

    /**
     *
     * @param pageVersion
     * @param pageFamily
     * @param id
     * @param version
     * @param maxCount
     * @param keySize
     * @param variableKeySizes
     * @param payloadSize
     * @param variablePayloadSizes
     * @param byteBufferProvider
     * @return
     */
    public MapChunk allocate(byte pageFamily,
            byte pageVersion,
            byte[] id,
            long version,
            int maxCount,
            int keySize,
            boolean variableKeySizes,
            int payloadSize,
            boolean variablePayloadSizes,
            ByteBufferProvider byteBufferProvider) {
        if (id == null || id.length != cIdSize) {
            throw new RuntimeException("Malformed ID");
        }
        int maxCapacity = (int) (maxCount + (maxCount - (maxCount * cSetDensity)));

        byte keyLengthSize = keyLengthSize(variableKeySizes ? keySize : 0);
        byte payloadLengthSize = keyLengthSize(variablePayloadSizes ? payloadSize : 0);

        int arraySize = cost(maxCount, keyLengthSize + keySize, payloadLengthSize + payloadSize);
        MapChunk page = new MapChunk(byteBufferProvider.allocate(arraySize));

        setPageFamily(page, pageFamily);
        setPageVersion(page, pageVersion);
        setId(page, id);
        setVersion(page, version);
        setCount(page, 0);
        setMaxCount(page, maxCount);
        setCapacity(page, maxCapacity); // good to use prime

        setKeySize(page, keySize);
        setKeyLengthSize(page, keyLengthSize);
        setPayloadSize(page, payloadSize);
        setPayloadLengthSize(page, payloadLengthSize);
        page.init(this);
        return page;
    }

    private byte keyLengthSize(int size) {
        if (size == 0) {
            return 0;
        } else if (size < Byte.MAX_VALUE) {
            return 1;
        } else if (size < Short.MAX_VALUE) {
            return 2;
        } else {
            return 4;
        }
    }

    /**
     *
     * @param page
     * @return
     */
    public byte getFamily(MapChunk page) {
        return read(page.array, cPageFamilyOffset);
    }

    /**
     *
     * @param page
     * @param family
     */
    public void setPageFamily(MapChunk page, byte family) {
        write(page.array, cPageFamilyOffset, family);
    }

    /**
     *
     * @param page
     * @return
     */
    public byte getPageVersion(MapChunk page) { //?? hacky
        return read(page.array, cPageVersionOffset);
    }

    /**
     *
     * @param page
     * @param family
     */
    public void setPageVersion(MapChunk page, byte family) {
        write(page.array, cPageVersionOffset, family);
    }

    /**
     *
     * @param page
     * @return
     */
    public byte[] getId(MapChunk page) {
        byte[] id = new byte[cIdSize];
        read(page.array, cIdOffset, id, 0, cIdSize);
        return id;
    }

    /**
     *
     * @param page
     * @param id
     */
    public void setId(MapChunk page, byte[] id) {
        write(page.array, cIdSize, id, 0, cIdSize);
    }

    /**
     *
     * @param page
     * @return
     */
    public long getVersion(MapChunk page) {
        return readLong(page.array, cVersionOffset);
    }

    /**
     *
     * @param page
     * @param version
     */
    public void setVersion(MapChunk page, long version) {
        write(page.array, cVersionOffset, longBytes(version, new byte[8], 0), 0, 8); // todo  refactor to use writeLong(
    }

    private byte[] longBytes(long v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 56);
        _bytes[_offset + 1] = (byte) (v >>> 48);
        _bytes[_offset + 2] = (byte) (v >>> 40);
        _bytes[_offset + 3] = (byte) (v >>> 32);
        _bytes[_offset + 4] = (byte) (v >>> 24);
        _bytes[_offset + 5] = (byte) (v >>> 16);
        _bytes[_offset + 6] = (byte) (v >>> 8);
        _bytes[_offset + 7] = (byte) v;
        return _bytes;
    }

    /**
     *
     * @param page
     * @return
     */
    public long getCount(MapChunk page) {
        return readInt(page.array, cCountOffset);
    }

    /**
     *
     * @param page
     * @return
     */
    public long getFreeCount(MapChunk page) {
        return getMaxCount(page) - getCount(page);
    }

    private void setCount(MapChunk page, long v) {
        writeInt(page.array, cCountOffset, (int) v);
    }

    /**
     *
     * @param page
     * @return
     */
    public int getMaxCount(MapChunk page) {
        return readInt(page.array, cMaxCountOffset);
    }

    private void setMaxCount(MapChunk page, int v) {
        writeInt(page.array, cMaxCountOffset, v);
    }

    /**
     *
     * @param page
     * @return
     */
    public int getCapacity(MapChunk page) {
        return readInt(page.array, cCapacityOffset);
    }

    private void setCapacity(MapChunk page, int v) {
        writeInt(page.array, cCapacityOffset, v);
    }

    /**
     *
     * @param page
     * @return
     */
    public int getKeySize(MapChunk page) {
        return readInt(page.array, cKeySizeOffset);
    }

    private void setKeySize(MapChunk page, int v) {
        writeInt(page.array, cKeySizeOffset, v);
    }

    public byte getKeyLengthSize(MapChunk page) {
        return read(page.array, cKeySizeVariableOffset);
    }

    private void setKeyLengthSize(MapChunk page, byte v) {
        write(page.array, cKeySizeVariableOffset, v);
    }

    /**
     *
     * @param page
     * @return
     */
    public int getPayloadSize(MapChunk page) {
        return readInt(page.array, cPayloadSizeOffset);
    }

    private void setPayloadSize(MapChunk page, int v) {
        writeInt(page.array, cPayloadSizeOffset, v);
    }

    public byte getPayloadLengthSize(MapChunk page) {
        return read(page.array, cPayloadSizeVariableOffset);
    }

    private void setPayloadLengthSize(MapChunk page, byte v) {
        write(page.array, cPayloadSizeVariableOffset, v);
    }

    private long index(long _arrayIndex, int entrySize) {
        return cHeaderSize + (1 + entrySize) * _arrayIndex;
    }

    public int add(MapChunk page, byte mode, byte[] key, byte[] payload) {
        return add(page, mode, key, 0, payload, 0);
    }

    public int add(MapChunk page, byte mode, long keyHash, byte[] key, byte[] payload) {
        return add(page, mode, keyHash, key, 0, payload, 0);
    }

    public int add(MapChunk page, byte mode, byte[] key, int keyOffset, byte[] payload, int _payloadOffset) {
        return add(page, mode, hash(key, keyOffset, key.length), key, keyOffset, payload, _payloadOffset);
    }

    public int add(MapChunk page, byte mode, long keyHash, byte[] key, int keyOffset, byte[] payload, int _payloadOffset) {
        int capacity = page.capacity;
        if (getCount(page) >= page.maxCount) {
            throw new OverCapacityException(getCount(page) + " > " + page.maxCount);
        }
        int keySize = page.keySize;
        int payloadSize = page.payloadSize;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
                j < k; // max search for available slot
                i = (++i) % k, j++) { // wraps around table

            long ai = index(i, page.entrySize);
            if (read(page.array, (int) ai) == cNull || read(page.array, (int) ai) == cSkip) {
                write(page.array, (int) ai, mode);
                write(page, (int) (ai + 1), page.keyLengthSize, key, keySize, keyOffset);
                write(page, (int) (ai + 1 + page.keyLengthSize + keySize), page.payloadLengthSize, payload, payloadSize, _payloadOffset);
                setCount(page, getCount(page) + 1);
                return (int) i;
            }
            if (equals(page.array, ai, page.keyLengthSize, key.length, key, keyOffset)) {
                write(page.array, (int) ai, mode);
                write(page, (int) (ai + 1 + page.keyLengthSize + keySize), page.payloadLengthSize, payload, payloadSize, _payloadOffset);
                return (int) i;
            }
        }
        return -1;
    }

    private void write(MapChunk page, int offest, int length, byte[] key, int size, int keyOffset) {

        if (length == 0) {
        } else if (length == 1) {
            write(page.array, offest, (byte) key.length);
        } else if (length == 2) {
            writeUnsignedShort(page.array, offest, key.length);
        } else if (length == 4) {
            writeInt(page.array, offest, key.length);
        } else {
            throw new RuntimeException("Unssuprted length. 0,1,2,4 valid but encounterd:" + length);
        }
        write(page.array, offest + length, key, keyOffset, key.length);

        int padding = size - key.length;
        if (padding > 0) {
            write(page.array, offest + length + key.length, new byte[padding], 0, padding);
        }
    }

    /**
     *
     * @param page
     * @param _key
     * @return
     */
    public boolean contains(MapChunk page, byte[] _key) {
        return get(page, _key) != -1;
    }

    /**
     *
     * @param setIndex
     * @param entrySize
     * @return
     */
    public int startOfKey(long setIndex, int entrySize) {
        return (int) (index(setIndex, entrySize) + 1);
    }

    /**
     *
     * @param page
     * @param i
     * @return
     */
    public byte[] getKeyAtIndex(MapChunk page, long i) {
        if (i < 0 || i >= page.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(page) - 1) + ")");
        }
        long ai = index(i, page.entrySize);
        if (read(page.array, (int) ai) == cSkip) {
            return null;
        }
        if (read(page.array, (int) ai) == cNull) {
            return null;
        }
        return getKey(page, i);
    }

    /**
     *
     * @param setIndex
     * @param keySize
     * @return
     */
    public long startOfPayload(long setIndex, int entrySize, int keyLength, int keySize) {
        long ai = index(setIndex, entrySize);
        return (ai + 1 + keyLength + keySize);
    }

    /**
     *
     * @param page
     * @param i
     * @return
     */
    public byte[] getPayloadAtIndex(MapChunk page, int i) {
        if (i < 0 || i >= page.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(page) - 1) + ")");
        }
        long ai = index(i, page.entrySize);
        if (read(page.array, (int) ai) == cSkip) {
            return null;
        }
        if (read(page.array, (int) ai) == cNull) {
            return null;
        }
        return getPayload(page, i);
    }

    /**
     *
     * @param page
     * @param i
     * @param _destOffset
     * @param payload
     * @param _poffset
     * @param _plength
     */
    public void setPayloadAtIndex(MapChunk page, long i, int _destOffset, byte[] payload, int _poffset, int _plength) {
        if (i < 0 || i >= page.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(page) - 1) + ")");
        }
        long ai = index(i, page.entrySize);
        if (read(page.array, (int) ai) == cSkip) {
            return;
        }
        if (read(page.array, (int) ai) == cNull) {
            return;
        }
        write(page, (int) (ai + 1 + page.keyLengthSize + page.keySize) + _destOffset, page.payloadLengthSize, payload, page.payloadSize, _poffset);
    }

    public byte[] getPayload(MapChunk page, byte[] key) {
        long i = get(page, key);
        return (i == -1) ? null : getPayload(page, i);
    }

    /**
     *
     * @param page
     * @param key
     * @return
     */
    public long get(MapChunk page, byte[] key) {
        return get(page, key, 0);
    }

    public long get(MapChunk page, long keyHash, byte[] key) {
        return get(page, keyHash, key, 0);
    }

    public long get(MapChunk page, byte[] key, int keyOffset) {
        return get(page, hash(key, keyOffset, key.length), key, keyOffset);
    }

    public long get(MapChunk page, long keyHash, byte[] key, int keyOffset) {
        if (key == null || key.length == 0) {
            return -1;
        }
        int entrySize = page.entrySize;
        int capacity = page.capacity;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
                j < k; // max search for key
                i = (++i) % k, j++) { // wraps around table

            long ai = index(i, entrySize);
            byte mode = read(page.array, (int) ai);
            if (mode == cSkip) {
                continue;
            }
            if (mode == cNull) {
                return -1;
            }
            if (equals(page.array, ai, page.keyLengthSize, key.length, key, keyOffset)) {
                return i;
            }
        }
        return -1;
    }

    public byte getMode(MapChunk page, long i) {
        long ai = index(i, page.entrySize);
        return read(page.array, ai);
    }

    public byte[] getKey(MapChunk page, long i) {
        long ai = index(i, page.entrySize);
        int length = length(page, page.keyLengthSize, page.keySize, ai + 1);
        byte[] k = new byte[length];
        read(page.array, (int) ai + 1 + page.keyLengthSize, k, 0, length);
        return k;
    }

    private int length(MapChunk page, byte lengthSize, int size, long i) {
        if (lengthSize == 0) {
            return size;
        } else if (lengthSize == 1) {
            return read(page.array, i);
        } else if (lengthSize == 2) {
            return readUnsignedShort(page.array, i);
        } else {
            return readInt(page.array, i);
        }
    }

    public byte[] getPayload(MapChunk page, long i) {
        long ai = index(i, page.entrySize);
        long offest = ai + 1 + page.keyLengthSize + page.keySize;
        int length = length(page, page.payloadLengthSize, page.payloadSize, offest);
        byte[] p = new byte[length];
        read(page.array, (int) offest + page.payloadLengthSize, p, 0, length);
        return p;
    }

    public int remove(MapChunk page, byte[] key) {
        return remove(page, key, 0);
    }

    public int remove(MapChunk page, long keyHash, byte[] key) {
        return remove(page, keyHash, key, 0);
    }

    public int remove(MapChunk page, byte[] key, int keyOffset) {
        return remove(page, hash(key, 0, key.length), key, keyOffset);
    }

    public int remove(MapChunk page, long keyHash, byte[] key, int keyOffset) {
        if (key == null || key.length == 0) {
            return -1;
        }
        int capacity = page.capacity;
        int entrySize = page.entrySize;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
                j < k; // max search for key
                i = (++i) % k, j++) { // wraps around table

            long ai = index(i, page.entrySize);
            if (read(page.array, (int) ai) == cSkip) {
                continue;
            }
            if (read(page.array, (int) ai) == cNull) {
                return -1;
            }
            if (equals(page.array, ai, page.keyLengthSize, key.length, key, keyOffset)) {
                long next = (i + 1) % k;
                if (read(page.array, (int) index(next, entrySize)) == cNull) {
                    for (long z = i; z >= 0; z--) {
                        if (read(page.array, (int) index(z, entrySize)) != cSkip) {
                            break;
                        }
                        write(page.array, (int) index(z, entrySize), cNull);
                    }
                    write(page.array, (int) index(i, entrySize), cNull);
                } else {
                    write(page.array, (int) index(i, entrySize), cSkip);
                }
                setCount(page, getCount(page) - 1);
                return (int) i;
            }
        }
        return -1;
    }

    /**
     *
     * @param <R>
     * @param <E>
     * @param page
     * @param _callback
     */
    public <R, E extends Exception> void get(MapChunk page, IndexStream<E> _callback) {
        try {
            int capacity = page.capacity;
            long count = getCount(page);
            for (int i = 0; i < capacity; i++) {
                long ai = index(i, page.entrySize);
                if (read(page.array, (int) ai) == cNull) {
                    continue;
                }
                if (read(page.array, (int) ai) == cSkip) {
                    continue;
                }
                count--;
                if (!_callback.stream(i)) {
                    break;
                }
                if (count < 0) {
                    break;
                }
            }
            _callback.stream(-1); // EOS
        } catch (Exception x) {
        }
    }

    /**
     * Used to gow or shrink a set
     *
     * @param from
     * @param to
     * @param stream
     */
    public void copyTo(MapChunk from, MapChunk to, CopyToStream stream) {
        int fcapacity = from.capacity;
        int fkeySize = from.keySize;
        int fpayloadSize = from.payloadSize;
        long fcount = getCount(from);

        int tkeySize = to.keySize;
        int tpayloadSize = to.payloadSize;
        long tcount = getCount(to);
        int tmaxCount = getMaxCount(to);

        if (fkeySize != tkeySize) {
            throw new RuntimeException("Miss matched keySizes " + fkeySize + " vs " + tkeySize);
        }
        if (fpayloadSize != tpayloadSize) {
            throw new RuntimeException("Miss matched payloadSize" + fpayloadSize + " vs " + tpayloadSize);
        }
        if (tmaxCount - tcount < fcount) {
            throw new RuntimeException("Insufficient room " + tmaxCount + " vs " + fcount);
        }

        for (int fromIndex = 0; fromIndex < fcapacity; fromIndex++) {
            long ai = index(fromIndex, from.entrySize);
            byte mode = read(from.array, (int) ai);
            if (mode == cNull) {
                continue;
            }
            if (mode == cSkip) {
                continue;
            }
            fcount--;
            int toIndex = add(to, mode, getKey(from, fromIndex), getPayload(from, fromIndex));

            if (stream != null) {
                stream.copied(fromIndex, toIndex);
            }

            if (fcount < 0) {
                break;
            }
        }
    }

    public interface CopyToStream {

        void copied(int fromIndex, int toIndex);
    }

    /**
     *
     * @param page
     */
    public void toSysOut(MapChunk page) {
        try {
            int capacity = page.capacity;
            int keySize = page.keySize;
            int payloadSize = page.payloadSize;
            for (int i = 0; i < capacity; i++) {
                long ai = index(i, page.entrySize);
                if (read(page.array, (int) ai) == cNull) {
                    System.out.println("\t" + i + "): null");
                    continue;
                }
                if (read(page.array, (int) ai) == cSkip) {
                    System.out.println("\t" + i + "): skip");
                    continue;
                }
                System.out.println("\t" + i + "): "
                        + Arrays.toString(getKey(page, i)) + "->"
                        + Arrays.toString(getPayload(page, i)));
            }
        } catch (Exception x) {
        }
    }

    /**
     *
     * @param _key
     * @param _start
     * @param _length
     * @return
     */
    public long hash(byte[] _key, int _start, int _length) {
        long hash = 0;
        long randMult = 0x5_DEEC_E66DL;
        long randAdd = 0xBL;
        long randMask = (1L << 48) - 1;
        long seed = _length;
        for (int i = 0; i < _length; i++) {
            long x = (seed * randMult + randAdd) & randMask;
            seed = x;
            hash += (_key[_start + i] + 128) * x;
        }
        return Math.abs(hash);
    }

    public Iterator<Entry> iterator(final MapChunk page) {
        return new Iterator<Entry>() {

            private int index = 0;

            private byte[] key;
            private byte[] payload;
            private int payloadIndex;

            @Override
            public boolean hasNext() {
                seekNext();
                return key != null;
            }

            @Override
            public Entry next() {
                seekNext();
                Entry entry = key != null ? new Entry(key, payload, payloadIndex) : null;
                key = null;
                payload = null;
                payloadIndex = -1;
                return entry;
            }

            private void seekNext() {
                while (index < page.capacity && key == null) {
                    key = getKeyAtIndex(page, index);
                    if (key != null) {
                        payload = getPayloadAtIndex(page, index);
                        payloadIndex = index;
                    }
                    index++;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static class Entry {

        public final byte[] key;
        public final byte[] payload;
        public final int payloadIndex;

        public Entry(byte[] key, byte[] payload, int payloadIndex) {
            this.key = key;
            this.payload = payload;
            this.payloadIndex = payloadIndex;
        }
    }

    /**
     * @param pageStart
     * @return
     */
    byte read(ByteBuffer array, long pageStart) {
        return array.get((int) pageStart);
    }

    /**
     * @param pageStart
     * @return
     */
    int readUnsingedByte(ByteBuffer array, long pageStart) {
        return array.get((int) pageStart);
    }

    /**
     * @param pageStart
     * @param v
     */
    void write(ByteBuffer array, long pageStart, byte v) {
        array.put((int) pageStart, v);
    }

    /**
     * @param pageStart
     * @return
     */
    int readShort(ByteBuffer array, long pageStart) {
        return array.getShort((int) pageStart);
    }

    /**
     * @param pageStart
     * @return
     */
    int readUnsignedShort(ByteBuffer array, long pageStart) {
        int v = 0;
        v |= (array.get((int) pageStart) & 0xFF);
        v <<= 8;
        v |= (array.get((int) pageStart + 1) & 0xFF);
        return v;
    }

    /**
     * @param pageStart
     * @return
     */
    int readInt(ByteBuffer array, long pageStart) {
        return array.getInt((int) pageStart);
    }

    /**
     * @param pageStart
     * @return
     */
    float readFloat(ByteBuffer array, long pageStart) {
        return array.getFloat((int) pageStart);
    }

    /**
     * @param pageStart
     * @return
     */
    long readLong(ByteBuffer array, long pageStart) {
        return array.getLong((int) pageStart);
    }

    /**
     * @param pageStart
     * @return
     */
    double readDouble(ByteBuffer array, long pageStart) {
        return array.getDouble((int) pageStart);
    }

    void writeUnsignedShort(ByteBuffer array, long pageStart, int v) {
        array.put((int) pageStart, (byte) (v >>> 8));
        array.put((int) pageStart + 1, (byte) v);
    }

    /**
     * @param pageStart
     * @param v
     */
    void writeInt(ByteBuffer array, long pageStart, int v) {
        array.putInt((int) pageStart, v);
    }

    /**
     * @param pageStart
     * @param read
     * @param offset
     * @param length
     */
    void read(ByteBuffer array, int pageStart, byte[] read, int offset, int length) {
        array.position(pageStart);
        array.get(read, offset, length);
    }

    /**
     * @param pageStart
     * @param towrite
     * @param offest
     * @param length
     */
    void write(ByteBuffer array, int pageStart, byte[] towrite, int offest, int length) {
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
    boolean equals(ByteBuffer array, long pageStart, int keyLength, int keySize, byte[] b, int boffset) {
        pageStart++; // remove mode byte
        if (keyLength == 0) {
        } else if (keyLength == 1) {
            if (array.get((int) (pageStart)) != keySize) {
                return false;
            }
            pageStart++;
        } else if (keyLength == 2) {
            if (readUnsignedShort(array, pageStart) != keySize) {
                return false;
            }
            pageStart += 2;
        } else if (keyLength == 4) {
            if (array.getInt((int) (pageStart)) != keySize) {
                return false;
            }
            pageStart += 4;
        } else {
            throw new RuntimeException("Unsupported keylength=" + keyLength);
        }
        for (int i = 0; i < keySize; i++) {
            int pageIndex = (int) (pageStart + i);
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
    boolean isLoaded(ByteBuffer array) {
        if (array instanceof MappedByteBuffer) {
            return ((MappedByteBuffer) array).isLoaded();
        }
        return true;
    }

    /**
     *
     */
    void force(ByteBuffer array) {
        if (array instanceof MappedByteBuffer) {
            ((MappedByteBuffer) array).force();
        }
    }

}
