package com.jivesoftware.os.filer.map.store;

//!! POORLY TESTING
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.map.store.extractors.Extractor;
import com.jivesoftware.os.filer.map.store.extractors.ExtractorStream;
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

    public final byte cPageFamily = 1;
    public final byte cPageVersion = 1;
    private final int cPageVersionSize = 1;
    private final int cPageFamilySize = 1;
    private final int cIdSize = 16;
    private final int cVersion = 8;
    private final int cCountSize = 4;
    private final int cMaxCountSize = 4;
    private final int cMaxCapacitySize = 4;
    private final int cKeySizeSize = 4;
    private final int cPayloadSize = 4;
    private final int cHeaderSize = cPageFamilySize + cPageVersionSize + cIdSize + cVersion + cCountSize
        + cMaxCountSize + cMaxCapacitySize + cKeySizeSize + cPayloadSize;
    private final int cPageFamilyOffset = 0;
    private final int cPageVersionOffset = cPageFamilySize;
    private final int cIdOffset = cPageFamilySize + cPageVersionSize;
    private final int cVersionOffset = cPageFamilySize + cPageVersionSize + cIdSize;
    private final int cCountOffset = cPageFamilySize + cPageVersionSize + cIdSize + cVersion;
    private final int cMaxCountOffset = cPageFamilySize + cPageVersionSize + cIdSize + cVersion + cCountSize;
    private final int cCapacityOffset = cPageFamilySize + cPageVersionSize + cIdSize + cVersion + cCountSize + cMaxCountSize;
    private final int cKeySizeOffset = cPageFamilySize + cPageVersionSize + cIdSize + cVersion + cCountSize + cMaxCountSize + cMaxCapacitySize;
    private final int cPayloadOffset = cPageFamilySize + cPageVersionSize + cIdSize + cVersion + cCountSize + cMaxCountSize + cMaxCapacitySize + cKeySizeSize;
    private final double cSetDensity = 0.6d;
    private final byte cSkip = -1;
    private final byte cNull = 0;

    private final Extractor<Integer> extractIndex;
    private final Extractor<byte[]> extractKey;
    private final Extractor<byte[]> extractPayload;

    public MapStore(Extractor<Integer> extractIndex, Extractor<byte[]> extractKey, Extractor<byte[]> extractPayload) {
        this.extractIndex = extractIndex;
        this.extractKey = extractKey;
        this.extractPayload = extractPayload;
    }

    /**
     *
     * @param _maxKeys
     * @param _keySize
     * @param _payloadSize
     * @return
     */
    public int cost(int _maxKeys, int _keySize, int _payloadSize) {
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
     * @param payloadSize
     * @param factory
     * @return
     */
    public MapChunk allocate(byte pageFamily,
        byte pageVersion,
        byte[] id,
        long version,
        int maxCount,
        int keySize,
        int payloadSize,
        ByteBufferFactory factory) {
        if (id == null || id.length != cIdSize) {
            throw new RuntimeException("Malformed ID");
        }
        int maxCapacity = (int) (maxCount + (maxCount - (maxCount * cSetDensity)));

        int arraySize = cost(maxCount, keySize, payloadSize);
        MapChunk page = new MapChunk(this, factory.allocate(arraySize));
        setPageFamily(page, pageFamily);
        setPageVersion(page, pageVersion);
        setId(page, id);
        setVersion(page, version);
        setCount(page, 0);
        setMaxCount(page, maxCount);
        setCapacity(page, maxCapacity); // good to use prime
        setKeySize(page, keySize);
        setPayloadSize(page, payloadSize);
        page.init();
        return page;
    }

    /**
     *
     * @param page
     * @return
     */
    public byte getFamily(MapChunk page) {
        return page.read(cPageFamilyOffset);
    }

    /**
     *
     * @param page
     * @param family
     */
    public void setPageFamily(MapChunk page, byte family) {
        page.write(cPageFamilyOffset, family);
    }

    /**
     *
     * @param page
     * @return
     */
    public byte getPageVersion(MapChunk page) { //?? hacky
        return page.read(cPageVersionOffset);
    }

    /**
     *
     * @param page
     * @param family
     */
    public void setPageVersion(MapChunk page, byte family) {
        page.write(cPageVersionOffset, family);
    }

    /**
     *
     * @param page
     * @return
     */
    public byte[] getId(MapChunk page) {
        byte[] id = new byte[cIdSize];
        page.read(cIdOffset, id, 0, cIdSize);
        return id;
    }

    /**
     *
     * @param page
     * @param id
     */
    public void setId(MapChunk page, byte[] id) {
        page.write(cIdSize, id, 0, cIdSize);
    }

    /**
     *
     * @param page
     * @return
     */
    public long getVersion(MapChunk page) {
        return page.readLong(cVersionOffset);
    }

    /**
     *
     * @param page
     * @param version
     */
    public void setVersion(MapChunk page, long version) {
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
     *
     * @param page
     * @return
     */
    public long getCount(MapChunk page) {
        return page.readInt(cCountOffset);
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
        page.writeInt(cCountOffset, (int) v);
    }

    /**
     *
     * @param page
     * @return
     */
    public int getMaxCount(MapChunk page) {
        return page.readInt(cMaxCountOffset);
    }

    private void setMaxCount(MapChunk page, int v) {
        page.writeInt(cMaxCountOffset, v);
    }

    /**
     *
     * @param page
     * @return
     */
    public int getCapacity(MapChunk page) {
        return page.readInt(cCapacityOffset);
    }

    private void setCapacity(MapChunk page, int v) {
        page.writeInt(cCapacityOffset, v);
    }

    /**
     *
     * @param page
     * @return
     */
    public int getKeySize(MapChunk page) {
        return page.readInt(cKeySizeOffset);
    }

    private void setKeySize(MapChunk page, int v) {
        page.writeInt(cKeySizeOffset, v);
    }

    /**
     *
     * @param page
     * @return
     */
    public int getPayloadSize(MapChunk page) {
        return page.readInt(cPayloadOffset);
    }

    private void setPayloadSize(MapChunk page, int v) {
        page.writeInt(cPayloadOffset, v);
    }

    private long index(long _arrayIndex, int keySize, int payloadSize) {
        return cHeaderSize + (1 + keySize + payloadSize) * _arrayIndex;
    }

    public int add(MapChunk page, byte mode, byte[] key, byte[] payload) {
        return add(page, mode, key, 0, payload, 0);
    }

    public int add(MapChunk page, byte mode, long keyHash, byte[] key, byte[] payload) {
        return add(page, mode, keyHash, key, 0, payload, 0);
    }

    public int add(MapChunk page, byte mode, byte[] key, int keyOffset, byte[] payload, int _payloadOffset) {
        int keySize = page.keySize;
        return add(page, mode, hash(key, keyOffset, keySize), key, keyOffset, payload, _payloadOffset);
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

            long ai = index(i, keySize, payloadSize);
            if (page.read((int) ai) == cNull || page.read((int) ai) == cSkip) {
                page.write((int) ai, mode);
                page.write((int) (ai + 1), key, keyOffset, keySize);
                //System.arraycopy(key, 0, page, (int) (ai + 1), keySize);
                page.write((int) (ai + 1 + keySize), payload, _payloadOffset, payloadSize);
                //System.arraycopy(_payload, 0, page, (int) (ai + 1 + keySize), payloadSize);
                setCount(page, getCount(page) + 1);
                return (int) i;
            }
            if (page.equals(ai, keySize, key, keyOffset)) {
                page.write((int) ai, mode);
                //byte[] oldPayload = payload(page,ai,keySize,payloadSize);
                page.write((int) (ai + 1 + keySize), payload, _payloadOffset, payloadSize);
                //System.arraycopy(_payload, 0, page, (int)(ai+1+keySize), payloadSize);
                return (int) i;
            }
        }
        return -1;
    }

    /**
     *
     * @param page
     * @param _key
     * @return
     */
    public boolean contains(MapChunk page, byte[] _key) {
        return get(page, _key, extractIndex) != -1;
    }

    /**
     *
     * @param setIndex
     * @param keySize
     * @param payloadSize
     * @return
     */
    public int startOfKey(int setIndex, int keySize, int payloadSize) {
        return (int) (index(setIndex, keySize, payloadSize) + 1);
    }

    /**
     *
     * @param page
     * @param i
     * @return
     */
    public byte[] getKeyAtIndex(MapChunk page, int i) {
        if (i < 0 || i >= page.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(page) - 1) + ")");
        }
        int keySize = page.keySize;
        int payloadSize = page.payloadSize;
        long ai = index(i, keySize, payloadSize);
        if (page.read((int) ai) == cSkip) {
            return null;
        }
        if (page.read((int) ai) == cNull) {
            return null;
        }
        return extractKey.extract(i, ai, keySize, payloadSize, page);
    }

    /**
     *
     * @param setIndex
     * @param keySize
     * @param payloadSize
     * @return
     */
    public int startOfPayload(int setIndex, int keySize, int payloadSize) {
        long ai = index(setIndex, keySize, payloadSize);
        return (int) (ai + 1 + keySize);
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
        int keySize = page.keySize;
        int payloadSize = page.payloadSize;
        long ai = index(i, keySize, payloadSize);
        if (page.read((int) ai) == cSkip) {
            return null;
        }
        if (page.read((int) ai) == cNull) {
            return null;
        }
        return extractPayload.extract(i, ai, keySize, payloadSize, page);
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
    public void setPayloadAtIndex(MapChunk page, int i, int _destOffset, byte[] payload, int _poffset, int _plength) {
        if (i < 0 || i >= page.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(page) - 1) + ")");
        }
        int keySize = page.keySize;
        int payloadSize = page.payloadSize;
        long ai = index(i, keySize, payloadSize);
        if (page.read((int) ai) == cSkip) {
            return;
        }
        if (page.read((int) ai) == cNull) {
            return;
        }
        page.write((int) (ai + 1 + keySize) + _destOffset, payload, _poffset, _plength);
    }

    /**
     *
     * @param <R>
     * @param page
     * @param key
     * @param extractor
     * @return
     */
    public <R> R get(MapChunk page, byte[] key, Extractor<R> extractor) {
        return get(page, key, 0, extractor);
    }

    public <R> R get(MapChunk page, long keyHash, byte[] key, Extractor<R> extractor) {
        return get(page, keyHash, key, 0, extractor);
    }

    public <R> R get(MapChunk page, byte[] key, int keyOffset, Extractor<R> extractor) {
        int keySize = page.keySize;
        return get(page, hash(key, keyOffset, keySize), key, keyOffset, extractor);
    }

    public <R> R get(MapChunk page, long keyHash, byte[] key, int keyOffset, Extractor<R> extractor) {
        if (key == null || key.length == 0) {
            return extractor.ifNull();
        }
        int capacity = page.capacity;
        int keySize = page.keySize;
        int payloadSize = page.payloadSize;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            long ai = index(i, keySize, payloadSize);
            if (page.read((int) ai) == cSkip) {
                continue;
            }
            if (page.read((int) ai) == cNull) {
                return extractor.ifNull();
            }
            if (page.equals(ai, keySize, key, keyOffset)) {
                return extractor.extract((int) i, ai, keySize, payloadSize, page);
            }
        }
        return extractor.ifNull();
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
        int keySize = page.keySize;
        int payloadSize = page.payloadSize;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            long ai = index(i, keySize, payloadSize);
            if (page.read((int) ai) == cSkip) {
                continue;
            }
            if (page.read((int) ai) == cNull) {
                return -1;
            }
            if (page.equals(ai, keySize, key, keyOffset)) {
                byte[] removedPayload = extractPayload.extract((int) i, ai, keySize, payloadSize, page);
                long next = (i + 1) % k;
                if (page.read((int) index(next, keySize, payloadSize)) == cNull) {
                    for (long z = i; z >= 0; z--) {
                        if (page.read((int) index(z, keySize, payloadSize)) != cSkip) {
                            break;
                        }
                        page.write((int) index(z, keySize, payloadSize), cNull);
                    }
                    page.write((int) index(i, keySize, payloadSize), cNull);
                } else {
                    page.write((int) index(i, keySize, payloadSize), cSkip);
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
     * @param _extractor
     * @param _callback
     */
    public <R, E extends Exception> void get(MapChunk page, Extractor<R> _extractor, ExtractorStream<R, E> _callback) {
        try {
            int capacity = page.capacity;
            int keySize = page.keySize;
            int payloadSize = page.payloadSize;
            long count = getCount(page);
            for (int i = 0; i < capacity; i++) {
                long ai = index(i, keySize, payloadSize);
                if (page.read((int) ai) == cNull) {
                    continue;
                }
                if (page.read((int) ai) == cSkip) {
                    continue;
                }
                count--;
                R v = _extractor.extract(i, ai, keySize, payloadSize, page);
                R back = _callback.stream(v);
                if (back != v) {
                    break;
                }
                if (count < 0) {
                    break;
                }
            }
            _callback.stream(null); // EOS
        } catch (Exception x) {
        }
    }

    /**
     * Used to gow or shrink a set
     *
     * @param from
     * @param to
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
            long ai = index(fromIndex, fkeySize, fpayloadSize);
            byte mode = from.read((int) ai);
            if (mode == cNull) {
                continue;
            }
            if (mode == cSkip) {
                continue;
            }
            fcount--;
            int toIndex = add(to, mode, extractKey.extract(fromIndex, ai, fkeySize, fpayloadSize, from), extractPayload.extract(fromIndex, ai, fkeySize,
                fpayloadSize, from));

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
                long ai = index(i, keySize, payloadSize);
                if (page.read((int) ai) == cNull) {
                    System.out.println("\t" + i + "): null");
                    continue;
                }
                if (page.read((int) ai) == cSkip) {
                    System.out.println("\t" + i + "): skip");
                    continue;
                }
                System.out.println("\t" + i + "): "
                    + extractKey.extract(i, ai, keySize, payloadSize, page) + "->"
                    + extractPayload.extract(i, ai, keySize, payloadSize, page));
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
}
