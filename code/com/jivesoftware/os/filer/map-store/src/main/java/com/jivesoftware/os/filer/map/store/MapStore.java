package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.map.store.extractors.IndexStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * this is a key+payload set that is backed buy a byte array. It is a fixed size set. It will not grow or shrink. You need to be aware and expect that your
 * system will cause the set to throw OverCapacityExceptions. The goal is to create a collection which will page to and from disk or net as fast as possible.
 * Nothing is synchronized to make it thread safe you need to synchronize higher up.
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
     * @param _maxKeys
     * @param _keySize
     * @param _payloadSize
     * @return
     */
    int cost(int _maxKeys, int _keySize, int _payloadSize) {
        int maxCapacity = calculateCapacity(_maxKeys);
        // 1+ for head of entry status byte. 0 and -1 reserved
        int entrySize = 1 + _keySize + _payloadSize;
        return cHeaderSize + (entrySize * maxCapacity);
    }

    /**
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

    public int calculateCapacity(int maxCount) {
        return (int) (maxCount + (maxCount - (maxCount * cSetDensity)));
    }

    public <F extends ConcurrentFiler> F allocateFiler(int maxCount,
        int keySize,
        boolean variableKeySizes,
        int payloadSize,
        boolean variablePayloadSizes,
        ConcurrentFilerProvider<F> concurrentFilerProvider) throws IOException {

        byte keyLengthSize = keyLengthSize(variableKeySizes ? keySize : 0);
        byte payloadLengthSize = keyLengthSize(variablePayloadSizes ? payloadSize : 0);

        int arraySize = cost(maxCount, keyLengthSize + keySize, payloadLengthSize + payloadSize);
        return concurrentFilerProvider.allocate(arraySize);
    }

    /**
     * @param <F>
     * @param pageVersion
     * @param pageFamily
     * @param id
     * @param version
     * @param maxCount
     * @param keySize
     * @param variableKeySizes
     * @param payloadSize
     * @param variablePayloadSizes
     * @return
     * @throws java.io.IOException
     */
    public <F extends ConcurrentFiler> MapChunk<F> bootstrapAllocatedFiler(
        byte pageFamily,
        byte pageVersion,
        byte[] id,
        long version,
        int maxCount,
        int keySize,
        boolean variableKeySizes,
        int payloadSize,
        boolean variablePayloadSizes,
        F filer) throws IOException {

        if (id == null || id.length != cIdSize) {
            throw new RuntimeException("Malformed ID");
        }
        int maxCapacity = calculateCapacity(maxCount);

        byte keyLengthSize = keyLengthSize(variableKeySizes ? keySize : 0);
        byte payloadLengthSize = keyLengthSize(variablePayloadSizes ? payloadSize : 0);

        MapChunk<F> page = new MapChunk<>(filer);

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
     * @param <F>
     * @param page
     * @return
     */
    public <F extends ConcurrentFiler> byte getFamily(MapChunk<F> page) throws IOException {
        return page.read(cPageFamilyOffset);
    }

    /**
     * @param page
     * @param family
     */
    public <F extends ConcurrentFiler> void setPageFamily(MapChunk<F> page, byte family) throws IOException {
        page.write(cPageFamilyOffset, family);
    }

    /**
     * @param page
     * @return
     */
    public <F extends ConcurrentFiler> byte getPageVersion(MapChunk<F> page) throws IOException {
        return page.read(cPageVersionOffset);
    }

    /**
     * @param page
     * @param family
     */
    public <F extends ConcurrentFiler> void setPageVersion(MapChunk<F> page, byte family) throws IOException {
        page.write(cPageVersionOffset, family);
    }

    /**
     * @param page
     * @return
     */
    public <F extends ConcurrentFiler> byte[] getId(MapChunk<F> page) throws IOException {
        byte[] id = new byte[cIdSize];
        page.read(cIdOffset, id, 0, cIdSize);
        return id;
    }

    /**
     * @param page
     * @param id
     */
    public <F extends ConcurrentFiler> void setId(MapChunk<F> page, byte[] id) throws IOException {
        page.write(cIdSize, id, 0, cIdSize);
    }

    /**
     * @param page
     * @return
     */
    public <F extends ConcurrentFiler> long getVersion(MapChunk<F> page) throws IOException {
        return page.readLong(cVersionOffset);
    }

    /**
     * @param page
     * @param version
     */
    public <F extends ConcurrentFiler> void setVersion(MapChunk<F> page, long version) throws IOException {
        page.write(cVersionOffset, longBytes(version, new byte[8], 0), 0, 8); // todo  refactor to use writeLong(
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
     * @param page
     * @return
     */
    public <F extends ConcurrentFiler> long getCount(MapChunk<F> page) throws IOException {
        return page.readInt(cCountOffset);
    }

    public <F extends ConcurrentFiler> boolean isFull(MapChunk<F> page) throws IOException {
        return getCount(page) >= page.maxCount;
    }

    public <F extends ConcurrentFiler> int nextGrowSize(MapChunk<F> page) throws IOException {
        return page.maxCount * 2;
    }

    /**
     * @param page
     * @return
     */
    public <F extends ConcurrentFiler> long getFreeCount(MapChunk<F> page) throws IOException {
        return getMaxCount(page) - getCount(page);
    }

    private <F extends ConcurrentFiler> void setCount(MapChunk<F> page, long v) throws IOException {
        page.writeInt(cCountOffset, (int) v);
    }

    /**
     * @param page
     * @return
     */
    public <F extends ConcurrentFiler> int getMaxCount(MapChunk<F> page) throws IOException {
        return page.readInt(cMaxCountOffset);
    }

    private <F extends ConcurrentFiler> void setMaxCount(MapChunk<F> page, int v) throws IOException {
        page.writeInt(cMaxCountOffset, v);
    }

    /**
     * @param page
     * @return
     */
    public <F extends ConcurrentFiler> int getCapacity(MapChunk<F> page) throws IOException {
        return page.readInt(cCapacityOffset);
    }

    private <F extends ConcurrentFiler> void setCapacity(MapChunk<F> page, int v) throws IOException {
        page.writeInt(cCapacityOffset, v);
    }

    /**
     * @param page
     * @return
     */
    public <F extends ConcurrentFiler> int getKeySize(MapChunk<F> page) throws IOException {
        return page.readInt(cKeySizeOffset);
    }

    private <F extends ConcurrentFiler> void setKeySize(MapChunk<F> page, int v) throws IOException {
        page.writeInt(cKeySizeOffset, v);
    }

    public <F extends ConcurrentFiler> byte getKeyLengthSize(MapChunk<F> page) throws IOException {
        return page.read(cKeySizeVariableOffset);
    }

    private <F extends ConcurrentFiler> void setKeyLengthSize(MapChunk<F> page, byte v) throws IOException {
        page.write(cKeySizeVariableOffset, v);
    }

    /**
     * @param page
     * @return
     */
    public <F extends ConcurrentFiler> int getPayloadSize(MapChunk<F> page) throws IOException {
        return page.readInt(cPayloadSizeOffset);
    }

    private <F extends ConcurrentFiler> void setPayloadSize(MapChunk<F> page, int v) throws IOException {
        page.writeInt(cPayloadSizeOffset, v);
    }

    public <F extends ConcurrentFiler> byte getPayloadLengthSize(MapChunk<F> page) throws IOException {
        return page.read(cPayloadSizeVariableOffset);
    }

    private <F extends ConcurrentFiler> void setPayloadLengthSize(MapChunk<F> page, byte v) throws IOException {
        page.write(cPayloadSizeVariableOffset, v);
    }

    private long index(long _arrayIndex, int entrySize) {
        return cHeaderSize + (1 + entrySize) * _arrayIndex;
    }

    public <F extends ConcurrentFiler> int add(MapChunk<F> page, byte mode, byte[] key, byte[] payload) throws IOException {
        return add(page, mode, key, 0, payload, 0);
    }

    public <F extends ConcurrentFiler> int add(MapChunk<F> page, byte mode, long keyHash, byte[] key, byte[] payload) throws IOException {
        return add(page, mode, keyHash, key, 0, payload, 0);
    }

    public <F extends ConcurrentFiler> int add(MapChunk<F> page, byte mode, byte[] key, int keyOffset, byte[] payload, int _payloadOffset) throws IOException {
        return add(page, mode, hash(key, keyOffset, key.length), key, keyOffset, payload, _payloadOffset);
    }

    public <F extends ConcurrentFiler> int add(MapChunk<F> page, byte mode, long keyHash, byte[] key, int keyOffset, byte[] payload, int _payloadOffset)
        throws IOException {
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
            if (page.read((int) ai) == cNull || page.read((int) ai) == cSkip) {
                page.write((int) ai, mode);
                write(page, (int) (ai + 1), page.keyLengthSize, key, keySize, keyOffset);
                write(page, (int) (ai + 1 + page.keyLengthSize + keySize), page.payloadLengthSize, payload, payloadSize, _payloadOffset);
                setCount(page, getCount(page) + 1);
                return (int) i;
            }
            if (equals(page, ai, page.keyLengthSize, key.length, key, keyOffset)) {
                page.write((int) ai, mode);
                write(page, (int) (ai + 1 + page.keyLengthSize + keySize), page.payloadLengthSize, payload, payloadSize, _payloadOffset);
                return (int) i;
            }
        }
        return -1;
    }

    private <F extends ConcurrentFiler> void write(MapChunk<F> page, int offest, int length, byte[] key, int size, int keyOffset) throws IOException {

        if (length == 0) {
        } else if (length == 1) {
            page.write(offest, (byte) key.length);
        } else if (length == 2) {
            page.writeUnsignedShort(offest, key.length);
        } else if (length == 4) {
            page.writeInt(offest, key.length);
        } else {
            throw new RuntimeException("Unssuprted length. 0,1,2,4 valid but encounterd:" + length);
        }
        page.write(offest + length, key, keyOffset, key.length);

        int padding = size - key.length;
        if (padding > 0) {
            page.write(offest + length + key.length, new byte[padding], 0, padding);
        }
    }

    /**
     * @param page
     * @param _key
     * @return
     */
    public <F extends ConcurrentFiler> boolean contains(MapChunk<F> page, byte[] _key) throws IOException {
        return get(page, _key) != -1;
    }

    /**
     * @param setIndex
     * @param entrySize
     * @return
     */
    public int startOfKey(long setIndex, int entrySize) {
        return (int) (index(setIndex, entrySize) + 1);
    }

    /**
     * @param page
     * @param i
     * @return
     */
    public <F extends ConcurrentFiler> byte[] getKeyAtIndex(MapChunk<F> page, long i) throws IOException {
        if (i < 0 || i >= page.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(page) - 1) + ")");
        }
        long ai = index(i, page.entrySize);
        if (page.read((int) ai) == cSkip) {
            return null;
        }
        if (page.read((int) ai) == cNull) {
            return null;
        }
        return getKey(page, i);
    }

    /**
     * @param setIndex
     * @param keySize
     * @return
     */
    public long startOfPayload(long setIndex, int entrySize, int keyLength, int keySize) {
        long ai = index(setIndex, entrySize);
        return (ai + 1 + keyLength + keySize);
    }

    /**
     * @param page
     * @param i
     * @return
     */
    public <F extends ConcurrentFiler> byte[] getPayloadAtIndex(MapChunk<F> page, int i) throws IOException {
        if (i < 0 || i >= page.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(page) - 1) + ")");
        }
        long ai = index(i, page.entrySize);
        if (page.read((int) ai) == cSkip) {
            return null;
        }
        if (page.read((int) ai) == cNull) {
            return null;
        }
        return getPayload(page, i);
    }

    /**
     * @param page
     * @param i
     * @param _destOffset
     * @param payload
     * @param _poffset
     * @param _plength
     */
    public <F extends ConcurrentFiler> void setPayloadAtIndex(MapChunk<F> page, long i, int _destOffset, byte[] payload, int _poffset, int _plength)
        throws IOException {
        if (i < 0 || i >= page.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(page) - 1) + ")");
        }
        long ai = index(i, page.entrySize);
        if (page.read((int) ai) == cSkip) {
            return;
        }
        if (page.read((int) ai) == cNull) {
            return;
        }
        write(page, (int) (ai + 1 + page.keyLengthSize + page.keySize) + _destOffset, page.payloadLengthSize, payload, page.payloadSize, _poffset);
    }

    public <F extends ConcurrentFiler> byte[] getPayload(MapChunk<F> page, byte[] key) throws IOException {
        long i = get(page, key);
        return (i == -1) ? null : getPayload(page, i);
    }

    /**
     * @param <F>
     * @param page
     * @param key
     * @return
     */
    public <F extends ConcurrentFiler> long get(MapChunk<F> page, byte[] key) throws IOException {
        return get(page, key, 0);
    }

    public <F extends ConcurrentFiler> long get(MapChunk<F> page, long keyHash, byte[] key) throws IOException {
        return get(page, keyHash, key, 0);
    }

    public <F extends ConcurrentFiler> long get(MapChunk<F> page, byte[] key, int keyOffset) throws IOException {
        return get(page, hash(key, keyOffset, key.length), key, keyOffset);
    }

    public <F extends ConcurrentFiler> long get(MapChunk<F> page, long keyHash, byte[] key, int keyOffset) throws IOException {
        if (key == null || key.length == 0) {
            return -1;
        }
        int entrySize = page.entrySize;
        int capacity = page.capacity;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            long ai = index(i, entrySize);
            byte mode = page.read((int) ai);
            if (mode == cSkip) {
                continue;
            }
            if (mode == cNull) {
                return -1;
            }
            if (equals(page, ai, page.keyLengthSize, key.length, key, keyOffset)) {
                return i;
            }
        }
        return -1;
    }

    public <F extends ConcurrentFiler> byte getMode(MapChunk<F> page, long i) throws IOException {
        long ai = index(i, page.entrySize);
        return page.read(ai);
    }

    public <F extends ConcurrentFiler> byte[] getKey(MapChunk<F> page, long i) throws IOException {
        long ai = index(i, page.entrySize);
        int length = length(page, page.keyLengthSize, page.keySize, ai + 1);
        byte[] k = new byte[length];
        page.read((int) ai + 1 + page.keyLengthSize, k, 0, length);
        return k;
    }

    private <F extends ConcurrentFiler> int length(MapChunk<F> page, byte lengthSize, int size, long i) throws IOException {
        if (lengthSize == 0) {
            return size;
        } else if (lengthSize == 1) {
            return page.read(i);
        } else if (lengthSize == 2) {
            return page.readUnsignedShort(i);
        } else {
            return page.readInt(i);
        }
    }

    public <F extends ConcurrentFiler> byte[] getPayload(MapChunk<F> page, long i) throws IOException {
        long ai = index(i, page.entrySize);
        long offest = ai + 1 + page.keyLengthSize + page.keySize;
        int length = length(page, page.payloadLengthSize, page.payloadSize, offest);
        byte[] p = new byte[length];
        page.read((int) offest + page.payloadLengthSize, p, 0, length);
        return p;
    }

    public <F extends ConcurrentFiler> int remove(MapChunk<F> page, byte[] key) throws IOException {
        return remove(page, key, 0);
    }

    public <F extends ConcurrentFiler> int remove(MapChunk<F> page, long keyHash, byte[] key) throws IOException {
        return remove(page, keyHash, key, 0);
    }

    public <F extends ConcurrentFiler> int remove(MapChunk<F> page, byte[] key, int keyOffset) throws IOException {
        return remove(page, hash(key, 0, key.length), key, keyOffset);
    }

    public <F extends ConcurrentFiler> int remove(MapChunk<F> page, long keyHash, byte[] key, int keyOffset) throws IOException {
        if (key == null || key.length == 0) {
            return -1;
        }
        int capacity = page.capacity;
        int entrySize = page.entrySize;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            long ai = index(i, page.entrySize);
            if (page.read((int) ai) == cSkip) {
                continue;
            }
            if (page.read((int) ai) == cNull) {
                return -1;
            }
            if (equals(page, ai, page.keyLengthSize, key.length, key, keyOffset)) {
                long next = (i + 1) % k;
                if (page.read((int) index(next, entrySize)) == cNull) {
                    for (long z = i; z >= 0; z--) {
                        if (page.read((int) index(z, entrySize)) != cSkip) {
                            break;
                        }
                        page.write((int) index(z, entrySize), cNull);
                    }
                    page.write((int) index(i, entrySize), cNull);
                } else {
                    page.write((int) index(i, entrySize), cSkip);
                }
                setCount(page, getCount(page) - 1);
                return (int) i;
            }
        }
        return -1;
    }

    /**
     * @param <R>
     * @param <E>
     * @param page
     * @param _callback
     */
    public <F extends ConcurrentFiler, R, E extends Exception> void get(MapChunk<F> page, IndexStream<E> _callback) {
        try {
            int capacity = page.capacity;
            long count = getCount(page);
            for (int i = 0; i < capacity; i++) {
                long ai = index(i, page.entrySize);
                if (page.read((int) ai) == cNull) {
                    continue;
                }
                if (page.read((int) ai) == cSkip) {
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
    public <F extends ConcurrentFiler> void copyTo(MapChunk<F> from, MapChunk<F> to, CopyToStream stream) throws IOException {
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
            byte mode = from.read((int) ai);
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
     * @param <F>
     * @param page
     * @throws java.io.IOException
     */
    public <F extends ConcurrentFiler> void toSysOut(MapChunk<F> page) throws IOException {
        try {
            int capacity = page.capacity;
            int keySize = page.keySize;
            int payloadSize = page.payloadSize;
            for (int i = 0; i < capacity; i++) {
                long ai = index(i, page.entrySize);
                if (page.read((int) ai) == cNull) {
                    System.out.println("\t" + i + "): null");
                    continue;
                }
                if (page.read((int) ai) == cSkip) {
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

    public <F extends ConcurrentFiler> Iterator<Entry> iterator(final MapChunk<F> page) {
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
                try {
                    while (index < page.capacity && key == null) {
                        key = getKeyAtIndex(page, index);
                        if (key != null) {
                            payload = getPayloadAtIndex(page, index);
                            payloadIndex = index;
                        }
                        index++;
                    }
                } catch (Exception x) {
                    throw new RuntimeException("Failed to seekNext:" + x);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public Iterator<byte[]> keysIterator(final MapChunk page) {
        return new Iterator<byte[]>() {

            private int index = 0;

            private byte[] key;

            @Override
            public boolean hasNext() {
                seekNext();
                return key != null;
            }

            @Override
            public byte[] next() {
                seekNext();
                byte[] nextKey = key;
                key = null;
                return nextKey;
            }

            private void seekNext() {
                try {
                    while (index < page.capacity && key == null) {
                        key = getKeyAtIndex(page, index);
                        index++;
                    }
                } catch (Exception x) {
                    throw new RuntimeException("Failed to seekNext:" + x);
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
     * @param keySize
     * @param b
     * @param boffset
     * @return
     */
    <F extends ConcurrentFiler> boolean equals(MapChunk<F> page, long pageStart, int keyLength, int keySize, byte[] b, int boffset) throws IOException {
        pageStart++; // remove mode byte
        if (keyLength == 0) {
        } else if (keyLength == 1) {
            if (page.read(pageStart) != keySize) {
                return false;
            }
            pageStart++;
        } else if (keyLength == 2) {
            if (page.readUnsignedShort(pageStart) != keySize) {
                return false;
            }
            pageStart += 2;
        } else if (keyLength == 4) {
            if (page.readInt((int) (pageStart)) != keySize) {
                return false;
            }
            pageStart += 4;
        } else {
            throw new RuntimeException("Unsupported keylength=" + keyLength);
        }
        for (int i = 0; i < keySize; i++) {
            int pageIndex = (int) (pageStart + i);
            if (page.read(pageIndex) != b[boffset + i]) {
                return false;
            }
        }
        return true;
    }
}
