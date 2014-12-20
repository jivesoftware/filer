package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.map.store.extractors.IndexStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * this is a key+payload set that is backed buy a byte array. It is a fixed size set. It will not grow or shrink. You need to be aware and expect that your
 * system will cause the set to throw OverCapacityExceptions. The goal is to create a collection which will context to and from disk or net as fast as possible.
 * Nothing is synchronized to make it thread safe you need to synchronize higher up.
 *
 * @author jonathan
 */
public class MapStore {

    public static final MapStore INSTANCE = new MapStore();

    public static final byte cVariableSized = 1;
    public static final byte cMapFamily = 1;
    public static final byte cMapVersion = 1;
    private static final int cMapVersionSize = 1;
    private static final int cMapFamilySize = 1;
    private static final int cIdSize = 16;
    private static final int cVersion = 8;
    private static final int cCountSize = 4;
    private static final int cMaxCountSize = 4;
    private static final int cMaxCapacitySize = 4;
    private static final int cKeySizeSize = 4;
    private static final int cPayloadSize = 4;

    private static final int cHeaderSize = cMapFamilySize + cMapVersionSize + cIdSize + cVersion + cCountSize
        + cMaxCountSize + cMaxCapacitySize + cVariableSized + cKeySizeSize + cVariableSized + cPayloadSize;

    private static final int cMapFamilyOffset = 0;
    private static final int cMapVersionOffset = cMapFamilySize;
    private static final int cIdOffset = cMapVersionOffset + cMapVersionSize;
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

    int cost(int _maxKeys, int _keySize, int _payloadSize) {
        int maxCapacity = calculateCapacity(_maxKeys);
        // 1+ for head of entry status byte. 0 and -1 reserved
        int entrySize = 1 + _keySize + _payloadSize;
        return cHeaderSize + (entrySize * maxCapacity);
    }

    public long absoluteMaxCount(int _keySize, int _payloadSize) {
        // 1+ for head of entry status byte. 0 and -1 reserved
        int entrySize = 1 + _keySize + _payloadSize;
        long maxCount = (Integer.MAX_VALUE - cHeaderSize) / entrySize;
        return (long) (maxCount * cSetDensity);
    }

    public int calculateCapacity(int maxCount) {
        return (int) (maxCount + (maxCount - (maxCount * cSetDensity)));
    }

    public int computeFilerSize(int maxCount,
        MapContext mapContext) throws IOException {
        return computeFilerSize(maxCount, mapContext.keySize,
            mapContext.keyLengthSize > 0,
            mapContext.payloadSize,
            mapContext.payloadLengthSize > 0);
    }

    public int computeFilerSize(int maxCount,
        int keySize,
        boolean variableKeySizes,
        int payloadSize,
        boolean variablePayloadSizes) throws IOException {

        byte keyLengthSize = keyLengthSize(variableKeySizes ? keySize : 0);
        byte payloadLengthSize = keyLengthSize(variablePayloadSizes ? payloadSize : 0);

        return cost(maxCount, keyLengthSize + keySize, payloadLengthSize + payloadSize);
    }

    public MapContext open(Filer filer) throws IOException {
        int keySize = getKeySize(filer);
        byte keyLengthSize = getKeyLengthSize(filer);
        int payloadSize = getPayloadSize(filer);
        byte payloadLengthSize = getPayloadLengthSize(filer);
        long count = getCount(filer);
        return new MapContext(keySize,
            keyLengthSize,
            payloadSize,
            payloadLengthSize,
            getCapacity(filer),
            getMaxCount(filer),
            keyLengthSize + keySize + payloadLengthSize + payloadSize,
            count);
    }

    public MapContext create(
        int maxCount,
        MapContext mapContext,
        Filer filer) throws IOException {
        return create(maxCount,
            mapContext.keySize,
            mapContext.keyLengthSize > 0,
            mapContext.payloadSize,
            mapContext.payloadLengthSize > 0,
            filer);
    }

    public MapContext create(
        int maxCount,
        int keySize,
        boolean variableKeySizes,
        int payloadSize,
        boolean variablePayloadSizes,
        Filer filer) throws IOException {

        byte[] id = new byte[cIdSize];
        int maxCapacity = calculateCapacity(maxCount);

        byte keyLengthSize = keyLengthSize(variableKeySizes ? keySize : 0);
        byte payloadLengthSize = keyLengthSize(variablePayloadSizes ? payloadSize : 0);

        setMapFamily(filer, cMapFamily);
        setMapVersion(filer, cMapVersion);
        setId(filer, id);
        setVersion(filer, 0);

        setMaxCount(filer, maxCount);
        setCapacity(filer, maxCapacity); // good to use prime

        setKeySize(filer, keySize);
        setKeyLengthSize(filer, keyLengthSize);
        setPayloadSize(filer, payloadSize);
        setPayloadLengthSize(filer, payloadLengthSize);

        MapContext context = new MapContext(keySize,
            keyLengthSize,
            payloadSize,
            payloadLengthSize,
            maxCapacity,
            maxCount,
            keyLengthSize + keySize + payloadLengthSize + payloadSize,
            0);
        setCount(context, filer, 0);
        return context;
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

    public byte getFamily(Filer filer) throws IOException {
        return read(filer, cMapFamilyOffset);
    }

    public void setMapFamily(Filer filer, byte family) throws IOException {
        write(filer, cMapFamilyOffset, family);
    }

    public byte getMapVersion(Filer filer) throws IOException {
        return read(filer, cMapVersionOffset);
    }

    public void setMapVersion(Filer filer, byte family) throws IOException {
        write(filer, cMapVersionOffset, family);
    }

    public byte[] getId(Filer filer) throws IOException {
        byte[] id = new byte[cIdSize];
        read(filer, cIdOffset, id, 0, cIdSize);
        return id;
    }

    public void setId(Filer filer, byte[] id) throws IOException {
        write(filer, cIdSize, id, 0, cIdSize);
    }

    public long getVersion(Filer filer) throws IOException {
        return readLong(filer, cVersionOffset);
    }

    public void setVersion(Filer filer, long version) throws IOException {
        write(filer, cVersionOffset, longBytes(version, new byte[8], 0), 0, 8); // todo  refactor to use writeLong(
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

    long getCount(Filer filer) throws IOException {
        return readInt(filer, cCountOffset);
    }

    public boolean isFull(Filer filer, MapContext context) throws IOException {
        return context.count >= context.maxCount;
    }

    public boolean isFullWithNMore(Filer filer, MapContext context, int nMore) throws IOException {
        return context.count + nMore >= context.maxCount;
    }

    public int nextGrowSize(MapContext context) throws IOException {
        return context.maxCount * 2;
    }

    public int nextGrowSize(MapContext context, int withRoomForNMore) throws IOException {
        int totalRoom = context.maxCount + withRoomForNMore;
        int size = context.maxCount * 2;
        while (size < totalRoom) {
            size *= 2;
        }
        return size;
    }

    private void setCount(MapContext context, Filer filer, long count) throws IOException {
        context.count = count;
        writeInt(filer, cCountOffset, (int) count);
    }

    public int getMaxCount(Filer filer) throws IOException {
        return readInt(filer, cMaxCountOffset);
    }

    private void setMaxCount(Filer filer, int v) throws IOException {
        writeInt(filer, cMaxCountOffset, v);
    }

    public int getCapacity(Filer filer) throws IOException {
        return readInt(filer, cCapacityOffset);
    }

    private void setCapacity(Filer filer, int v) throws IOException {
        writeInt(filer, cCapacityOffset, v);
    }

    public int getKeySize(Filer filer) throws IOException {
        return readInt(filer, cKeySizeOffset);
    }

    private void setKeySize(Filer filer, int v) throws IOException {
        writeInt(filer, cKeySizeOffset, v);
    }

    public byte getKeyLengthSize(Filer filer) throws IOException {
        return read(filer, cKeySizeVariableOffset);
    }

    private void setKeyLengthSize(Filer filer, byte v) throws IOException {
        write(filer, cKeySizeVariableOffset, v);
    }

    public int getPayloadSize(Filer filer) throws IOException {
        return readInt(filer, cPayloadSizeOffset);
    }

    private void setPayloadSize(Filer filer, int v) throws IOException {
        writeInt(filer, cPayloadSizeOffset, v);
    }

    public byte getPayloadLengthSize(Filer filer) throws IOException {
        return read(filer, cPayloadSizeVariableOffset);
    }

    private void setPayloadLengthSize(Filer filer, byte v) throws IOException {
        write(filer, cPayloadSizeVariableOffset, v);
    }

    private long index(long _arrayIndex, int entrySize) {
        return cHeaderSize + (1 + entrySize) * _arrayIndex;
    }

    public int add(Filer filer, MapContext context, byte mode, byte[] key, byte[] payload) throws IOException {
        return add(filer, context, mode, key, 0, payload, 0);
    }

    public int add(Filer filer, MapContext context, byte mode, long keyHash, byte[] key, byte[] payload) throws IOException {
        return add(filer, context, mode, keyHash, key, 0, payload, 0);
    }

    public int add(Filer filer, MapContext context, byte mode, byte[] key, int keyOffset, byte[] payload, int _payloadOffset) throws IOException {
        return add(filer, context, mode, hash(key, keyOffset, key.length), key, keyOffset, payload, _payloadOffset);
    }

    public int add(Filer filer, MapContext context, byte mode, long keyHash, byte[] key, int keyOffset, byte[] payload, int _payloadOffset)
        throws IOException {
        int capacity = context.capacity;
        if (context.count >= context.maxCount) {
            throw new OverCapacityException(context.count + " > " + context.maxCount);
        }
        int keySize = context.keySize;
        int payloadSize = context.payloadSize;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for available slot
            i = (++i) % k, j++) { // wraps around table

            long ai = index(i, context.entrySize);
            if (read(filer, (int) ai) == cNull || read(filer, (int) ai) == cSkip) {
                write(filer, (int) ai, mode);
                write(filer, (int) (ai + 1), context.keyLengthSize, key, keySize, keyOffset);
                write(filer, (int) (ai + 1 + context.keyLengthSize + keySize), context.payloadLengthSize, payload, payloadSize, _payloadOffset);
                setCount(context, filer, context.count + 1);
                return (int) i;
            }
            if (equals(filer, ai, context.keyLengthSize, key.length, key, keyOffset)) {
                write(filer, (int) ai, mode);
                write(filer, (int) (ai + 1 + context.keyLengthSize + keySize), context.payloadLengthSize, payload, payloadSize, _payloadOffset);
                return (int) i;
            }
        }
        return -1;
    }

    private void write(Filer filer, int offest, int length, byte[] key, int size, int keyOffset) throws IOException {

        if (length == 0) {
        } else if (length == 1) {
            write(filer, offest, (byte) key.length);
        } else if (length == 2) {
            writeUnsignedShort(filer, offest, key.length);
        } else if (length == 4) {
            writeInt(filer, offest, key.length);
        } else {
            throw new RuntimeException("Unssuprted length. 0,1,2,4 valid but encounterd:" + length);
        }
        write(filer, offest + length, key, keyOffset, key.length);

        int padding = size - key.length;
        if (padding > 0) {
            write(filer, offest + length + key.length, new byte[padding], 0, padding);
        }
    }

    public boolean contains(Filer filer, MapContext context, byte[] _key) throws IOException {
        return get(filer, context, _key) != -1;
    }

    public int startOfKey(long setIndex, int entrySize) {
        return (int) (index(setIndex, entrySize) + 1);
    }

    public byte[] getKeyAtIndex(Filer filer, MapContext context, long i) throws IOException {
        if (i < 0 || i >= context.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(filer) - 1) + ")");
        }
        long ai = index(i, context.entrySize);
        if (read(filer, (int) ai) == cSkip) {
            return null;
        }
        if (read(filer, (int) ai) == cNull) {
            return null;
        }
        return getKey(filer, context, i);
    }

    public long startOfPayload(long setIndex, int entrySize, int keyLength, int keySize) {
        long ai = index(setIndex, entrySize);
        return (ai + 1 + keyLength + keySize);
    }

    public byte[] getPayloadAtIndex(Filer filer, MapContext context, int i) throws IOException {
        if (i < 0 || i >= context.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(filer) - 1) + ")");
        }
        long ai = index(i, context.entrySize);
        if (read(filer, (int) ai) == cSkip) {
            return null;
        }
        if (read(filer, (int) ai) == cNull) {
            return null;
        }
        return getPayload(filer, context, i);
    }

    public void setPayloadAtIndex(Filer filer, MapContext context, long i, int _destOffset, byte[] payload, int _poffset, int _plength)
        throws IOException {
        if (i < 0 || i >= context.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(filer) - 1) + ")");
        }
        long ai = index(i, context.entrySize);
        if (read(filer, (int) ai) == cSkip) {
            return;
        }
        if (read(filer, (int) ai) == cNull) {
            return;
        }
        write(filer, (int) (ai + 1 + context.keyLengthSize + context.keySize) + _destOffset, context.payloadLengthSize, payload, context.payloadSize, _poffset);
    }

    public byte[] getPayload(Filer filer, MapContext context, byte[] key) throws IOException {
        long i = get(filer, context, key);
        return (i == -1) ? null : getPayload(filer, context, i);
    }

    public long get(Filer filer, MapContext context, byte[] key) throws IOException {
        return get(filer, context, key, 0);
    }

    public long get(Filer filer, MapContext context, long keyHash, byte[] key) throws IOException {
        return get(filer, context, keyHash, key, 0);
    }

    public long get(Filer filer, MapContext context, byte[] key, int keyOffset) throws IOException {
        return get(filer, context, hash(key, keyOffset, key.length), key, keyOffset);
    }

    public long get(Filer filer, MapContext context, long keyHash, byte[] key, int keyOffset) throws IOException {
        if (key == null || key.length == 0) {
            return -1;
        }
        int entrySize = context.entrySize;
        int capacity = context.capacity;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            long ai = index(i, entrySize);
            byte mode = read(filer, (int) ai);
            if (mode == cSkip) {
                continue;
            }
            if (mode == cNull) {
                return -1;
            }
            if (equals(filer, ai, context.keyLengthSize, key.length, key, keyOffset)) {
                return i;
            }
        }
        return -1;
    }

    public byte getMode(Filer filer, MapContext context, long i) throws IOException {
        long ai = index(i, context.entrySize);
        return read(filer, ai);
    }

    public byte[] getKey(Filer filer, MapContext context, long i) throws IOException {
        long ai = index(i, context.entrySize);
        int length = length(filer, context.keyLengthSize, context.keySize, ai + 1);
        byte[] k = new byte[length];
        read(filer, (int) ai + 1 + context.keyLengthSize, k, 0, length);
        return k;
    }

    private int length(Filer filer, byte lengthSize, int size, long i) throws IOException {
        if (lengthSize == 0) {
            return size;
        } else if (lengthSize == 1) {
            return read(filer, i);
        } else if (lengthSize == 2) {
            return readUnsignedShort(filer, i);
        } else {
            return readInt(filer, i);
        }
    }

    public byte[] getPayload(Filer filer, MapContext context, long i) throws IOException {
        long ai = index(i, context.entrySize);
        long offest = ai + 1 + context.keyLengthSize + context.keySize;
        int length = length(filer, context.payloadLengthSize, context.payloadSize, offest);
        byte[] p = new byte[length];
        read(filer, (int) offest + context.payloadLengthSize, p, 0, length);
        return p;
    }

    public int remove(Filer filer, MapContext context, byte[] key) throws IOException {
        return remove(filer, context, key, 0);
    }

    public int remove(Filer filer, MapContext context, long keyHash, byte[] key) throws IOException {
        return remove(filer, context, keyHash, key, 0);
    }

    public int remove(Filer filer, MapContext context, byte[] key, int keyOffset) throws IOException {
        return remove(filer, context, hash(key, 0, key.length), key, keyOffset);
    }

    public int remove(Filer filer, MapContext context, long keyHash, byte[] key, int keyOffset) throws IOException {
        if (key == null || key.length == 0) {
            return -1;
        }
        int capacity = context.capacity;
        int entrySize = context.entrySize;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            long ai = index(i, context.entrySize);
            if (read(filer, (int) ai) == cSkip) {
                continue;
            }
            if (read(filer, (int) ai) == cNull) {
                return -1;
            }
            if (equals(filer, ai, context.keyLengthSize, key.length, key, keyOffset)) {
                long next = (i + 1) % k;
                if (read(filer, (int) index(next, entrySize)) == cNull) {
                    for (long z = i; z >= 0; z--) {
                        if (read(filer, (int) index(z, entrySize)) != cSkip) {
                            break;
                        }
                        write(filer, (int) index(z, entrySize), cNull);
                    }
                    write(filer, (int) index(i, entrySize), cNull);
                } else {
                    write(filer, (int) index(i, entrySize), cSkip);
                }
                setCount(context, filer, context.count - 1);
                return (int) i;
            }
        }
        return -1;
    }

    public <E extends Exception> void get(Filer filer, MapContext context, IndexStream<E> _callback) {
        try {
            int capacity = context.capacity;
            long count = context.count;
            for (int i = 0; i < capacity; i++) {
                long ai = index(i, context.entrySize);
                if (read(filer, (int) ai) == cNull) {
                    continue;
                }
                if (read(filer, (int) ai) == cSkip) {
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

    public void copyTo(Filer fromFiler,
        MapContext fromContext,
        Filer toFiler,
        MapContext toContext,
        CopyToStream stream) throws IOException {

        int fcapacity = fromContext.capacity;
        int fkeySize = fromContext.keySize;
        int fpayloadSize = fromContext.payloadSize;
        long fcount = fromContext.count;

        int tkeySize = toContext.keySize;
        int tpayloadSize = toContext.payloadSize;
        long tcount = toContext.count;
        int tmaxCount = getMaxCount(toFiler);

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
            long ai = index(fromIndex, fromContext.entrySize);
            byte mode = read(fromFiler, (int) ai);
            if (mode == cNull) {
                continue;
            }
            if (mode == cSkip) {
                continue;
            }
            fcount--;
            byte[] key = getKey(fromFiler, fromContext, fromIndex);
            int toIndex = add(toFiler, toContext, mode, key, getPayload(fromFiler, fromContext, fromIndex));

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

    private void toSysOut(Filer filer, MapContext context) throws IOException {
        try {
            int capacity = context.capacity;
            for (int i = 0; i < capacity; i++) {
                long ai = index(i, context.entrySize);
                if (read(filer, (int) ai) == cNull) {
                    System.out.println("\t" + i + "): null");
                    continue;
                }
                if (read(filer, (int) ai) == cSkip) {
                    System.out.println("\t" + i + "): skip");
                    continue;
                }
                System.out.println("\t" + i + "): "
                    + Arrays.toString(getKey(filer, context, i)) + "->"
                    + Arrays.toString(getPayload(filer, context, i)));
            }
        } catch (Exception x) {
        }
    }

    private long hash(byte[] _key, int _start, int _length) {
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

    public boolean stream(final Filer filer, final MapContext context, EntryStream stream) throws IOException {
        for (int index = 0; index < context.capacity; index++) {
            byte[] key = getKeyAtIndex(filer, context, index);
            if (key != null) {
                byte[] payload = getPayloadAtIndex(filer, context, index);
                if (!stream.stream(new Entry(key, payload, index))) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean streamKeys(final Filer filer, final MapContext context, KeyStream stream) throws IOException {
        for (int index = 0; index < context.capacity; index++) {
            byte[] key = getKeyAtIndex(filer, context, index);
            if (key != null) {
                if (!stream.stream(key)) {
                    return false;
                }
            }
        }
        return true;
    }

    public interface EntryStream {

        boolean stream(Entry entry) throws IOException;
    }

    public interface KeyStream {

        boolean stream(byte[] key) throws IOException;
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

    private boolean equals(Filer filer, long start, int keyLength, int keySize, byte[] b, int boffset) throws IOException {
        start++; // remove mode byte
        if (keyLength == 0) {
        } else if (keyLength == 1) {
            if (read(filer, start) != keySize) {
                return false;
            }
            start++;
        } else if (keyLength == 2) {
            if (readUnsignedShort(filer, start) != keySize) {
                return false;
            }
            start += 2;
        } else if (keyLength == 4) {
            if (readInt(filer, (int) (start)) != keySize) {
                return false;
            }
            start += 4;
        } else {
            throw new RuntimeException("Unsupported keylength=" + keyLength);
        }
        for (int i = 0; i < keySize; i++) {
            int contextIndex = (int) (start + i);
            if (read(filer, contextIndex) != b[boffset + i]) {
                return false;
            }
        }
        return true;
    }

    byte read(Filer filer, long start) throws IOException {
        filer.seek(start);
        return (byte) filer.read();
    }

    int readUnsignedByte(Filer filer, long start) throws IOException {
        filer.seek(start);
        return (byte) filer.read();
    }

    void write(Filer filer, long start, byte v) throws IOException {
        filer.seek(start);
        filer.write(v);
    }

    int readShort(Filer filer, long start) throws IOException {
        filer.seek(start);
        byte[] bytes = new byte[2];
        filer.read(bytes);
        short v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    int readUnsignedShort(Filer filer, long start) throws IOException {
        filer.seek(start);
        byte[] bytes = new byte[2];
        filer.read(bytes);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    int readInt(Filer filer, long start) throws IOException {
        filer.seek(start);
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

    float readFloat(Filer filer, long start) throws IOException {
        filer.seek(start);
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

    long readLong(Filer filer, long start) throws IOException {
        filer.seek(start);
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

    double readDouble(Filer filer, long start) throws IOException {
        filer.seek(start);
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

    void writeUnsignedShort(Filer filer, long start, int v) throws IOException {
        filer.seek(start);
        filer.write(new byte[]{
            (byte) (v >>> 8),
            (byte) v
        });
    }

    void writeInt(Filer filer, long start, int v) throws IOException {
        filer.seek(start);
        filer.write(new byte[]{
            (byte) (v >>> 24),
            (byte) (v >>> 16),
            (byte) (v >>> 8),
            (byte) v
        });
    }

    void read(Filer filer, int start, byte[] read, int offset, int length) throws IOException {
        filer.seek(start);
        filer.read(read, offset, length);
    }

    void write(Filer filer, int start, byte[] towrite, int offest, int length) throws IOException {
        filer.seek(start);
        filer.write(towrite, offest, length);
    }

}
