package com.jivesoftware.os.filer.io.map;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.api.StackBuffer;
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
    public static final byte cMapVersion = 4;

    private static final int cCountSize = 4;
    private static final int cMaxCountSize = 4;
    private static final int cMaxCapacitySize = 4;
    private static final int cKeySizeSize = 4;
    private static final int cPayloadSize = 4;

    private static final int cHeaderSize = 1 + cCountSize + cMaxCountSize + cMaxCapacitySize + cVariableSized + cKeySizeSize + cVariableSized + cPayloadSize;

    private static final int cMapVersionOffset = 0;
    private static final int cCountOffset = cMapVersionOffset + 1;
    private static final int cMaxCountOffset = cCountOffset + cCountSize;
    private static final int cCapacityOffset = cMaxCountOffset + cMaxCountSize;
    private static final int cKeySizeOffset = cCapacityOffset + cMaxCapacitySize;
    private static final int cKeySizeVariableOffset = cKeySizeOffset + cKeySizeSize;
    private static final int cPayloadSizeOffset = cKeySizeVariableOffset + cVariableSized;
    private static final int cPayloadSizeVariableOffset = cPayloadSizeOffset + cPayloadSize;

    private static final double cSetDensity = 0.6d;
    static final byte cSkip = -1;
    static final byte cNull = 0;

    private MapStore() {
    }

    long cost(int _maxKeys, int _keySize, int _payloadSize) {
        int maxCapacity = calculateCapacity(_maxKeys);
        // 1+ for head of entry status byte. 0 and -1 reserved
        long entrySize = 1 + _keySize + _payloadSize;
        return cHeaderSize + (entrySize * maxCapacity);
    }

    public long absoluteMaxCount(int _keySize, int _payloadSize) {
        // 1+ for head of entry status byte. 0 and -1 reserved
        int entrySize = 1 + _keySize + _payloadSize;
        long maxCount = (Integer.MAX_VALUE - cHeaderSize) / entrySize;
        return (long) (maxCount * cSetDensity);
    }

    public static int calculateCapacity(int maxCount) {
        return (int) (maxCount + (maxCount - (maxCount * cSetDensity)));
    }

    public long computeFilerSize(int maxCount,
        MapContext mapContext) throws IOException {
        return computeFilerSize(maxCount, mapContext.keySize,
            mapContext.keyLengthSize > 0,
            mapContext.payloadSize,
            mapContext.payloadLengthSize > 0);
    }

    public long computeFilerSize(int maxCount,
        int keySize,
        boolean variableKeySizes,
        int payloadSize,
        boolean variablePayloadSizes) throws IOException {

        byte keyLengthSize = keyLengthSize(variableKeySizes ? keySize : 0);
        byte payloadLengthSize = keyLengthSize(variablePayloadSizes ? payloadSize : 0);

        return cost(maxCount, keyLengthSize + keySize, payloadLengthSize + payloadSize);
    }

    public MapContext open(Filer filer, StackBuffer stackBuffer) throws IOException {
        int keySize = getKeySize(filer, stackBuffer);
        byte keyLengthSize = getKeyLengthSize(filer);
        int payloadSize = getPayloadSize(filer, stackBuffer);
        byte payloadLengthSize = getPayloadLengthSize(filer);
        long count = getCount(filer, stackBuffer);
        byte version = getMapVersion(filer);
        return new MapContext(keySize,
            keyLengthSize,
            payloadSize,
            payloadLengthSize,
            getCapacity(filer, stackBuffer),
            getMaxCount(filer, stackBuffer),
            keyLengthSize + keySize + payloadLengthSize + payloadSize,
            version,
            count);
    }

    public MapContext create(
        int maxCount,
        MapContext mapContext,
        Filer filer,
        StackBuffer stackBuffer) throws IOException {
        return create(maxCount,
            mapContext.keySize,
            mapContext.keyLengthSize > 0,
            mapContext.payloadSize,
            mapContext.payloadLengthSize > 0,
            filer,
            stackBuffer);
    }

    public MapContext create(
        int maxCount,
        int keySize,
        boolean variableKeySizes,
        int payloadSize,
        boolean variablePayloadSizes,
        Filer filer,
        StackBuffer stackBuffer) throws IOException {

        int maxCapacity = calculateCapacity(maxCount);

        byte keyLengthSize = keyLengthSize(variableKeySizes ? keySize : 0);
        byte payloadLengthSize = keyLengthSize(variablePayloadSizes ? payloadSize : 0);

        setMapVersion(filer, cMapVersion);

        setMaxCount(filer, maxCount, stackBuffer);
        setCapacity(filer, maxCapacity, stackBuffer); // good to use prime

        setKeySize(filer, keySize, stackBuffer);
        setKeyLengthSize(filer, keyLengthSize);
        setPayloadSize(filer, payloadSize, stackBuffer);
        setPayloadLengthSize(filer, payloadLengthSize);

        MapContext context = new MapContext(keySize,
            keyLengthSize,
            payloadSize,
            payloadLengthSize,
            maxCapacity,
            maxCount,
            keyLengthSize + keySize + payloadLengthSize + payloadSize,
            cMapVersion,
            0);
        setCount(context, filer, 0, stackBuffer);
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

    public byte getMapVersion(Filer filer) throws IOException {
        return read(filer, cMapVersionOffset);
    }

    public void setMapVersion(Filer filer, byte family) throws IOException {
        write(filer, cMapVersionOffset, family);
    }

    public long getCount(Filer filer, StackBuffer stackBuffer) throws IOException {
        return readInt(filer, cCountOffset, stackBuffer);
    }

    public long getApproxCount(MapContext context) throws IOException {
        return context.count;
    }

    public boolean isFull(MapContext context) throws IOException {
        return context.count >= context.maxCount;
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

    public boolean acquire(MapContext context, int n) {
        context.requested += n;
        //System.out.println("requested: " + context.requested + " max:" + context.maxCount + " count:" + context.count);
        if (context.version < MapStore.cMapVersion) {
            return false;
        }
        return (context.requested <= (context.maxCount - context.count));
    }

    public void release(MapContext context, int n) {
        context.requested -= n;
    }

    private void setCount(MapContext context, Filer filer, long count, StackBuffer stackBuffer) throws IOException {
        context.count = count;
        writeInt(filer, cCountOffset, (int) count, stackBuffer);
    }

    public int getMaxCount(Filer filer, StackBuffer stackBuffer) throws IOException {
        return readInt(filer, cMaxCountOffset, stackBuffer);
    }

    private void setMaxCount(Filer filer, int v, StackBuffer stackBuffer) throws IOException {
        writeInt(filer, cMaxCountOffset, v, stackBuffer);
    }

    public int getCapacity(Filer filer, StackBuffer stackBuffer) throws IOException {
        return readInt(filer, cCapacityOffset, stackBuffer);
    }

    private void setCapacity(Filer filer, int v, StackBuffer stackBuffer) throws IOException {
        writeInt(filer, cCapacityOffset, v, stackBuffer);
    }

    public int getKeySize(Filer filer, StackBuffer stackBuffer) throws IOException {
        return readInt(filer, cKeySizeOffset, stackBuffer);
    }

    private void setKeySize(Filer filer, int v, StackBuffer stackBuffer) throws IOException {
        writeInt(filer, cKeySizeOffset, v, stackBuffer);
    }

    public byte getKeyLengthSize(Filer filer) throws IOException {
        return read(filer, cKeySizeVariableOffset);
    }

    private void setKeyLengthSize(Filer filer, byte v) throws IOException {
        write(filer, cKeySizeVariableOffset, v);
    }

    public int getPayloadSize(Filer filer, StackBuffer stackBuffer) throws IOException {
        return readInt(filer, cPayloadSizeOffset, stackBuffer);
    }

    private void setPayloadSize(Filer filer, int v, StackBuffer stackBuffer) throws IOException {
        writeInt(filer, cPayloadSizeOffset, v, stackBuffer);
    }

    public byte getPayloadLengthSize(Filer filer) throws IOException {
        return read(filer, cPayloadSizeVariableOffset);
    }

    private void setPayloadLengthSize(Filer filer, byte v) throws IOException {
        write(filer, cPayloadSizeVariableOffset, v);
    }

    long index(long _arrayIndex, int entrySize) {
        return cHeaderSize + (1 + entrySize) * _arrayIndex;
    }

    public long add(Filer filer, MapContext context, byte mode, byte[] key, byte[] payload, StackBuffer stackBuffer) throws IOException {
        return add(filer, context, mode, key, 0, payload, 0, stackBuffer);
    }

    public long add(Filer filer, MapContext context, byte mode, long keyHash, byte[] key, byte[] payload, StackBuffer stackBuffer) throws IOException {
        return add(filer, context, mode, keyHash, key, 0, payload, 0, stackBuffer);
    }

    public long add(Filer filer, MapContext context, byte mode, byte[] key, int keyOffset, byte[] payload, int _payloadOffset, StackBuffer stackBuffer) throws
        IOException {
        return add(filer, context, mode, hash(key, keyOffset, key.length), key, keyOffset, payload, _payloadOffset, stackBuffer);
    }

    public long add(Filer filer, MapContext context, byte mode, long keyHash, byte[] key, int keyOffset, byte[] payload, int _payloadOffset,
        StackBuffer stackBuffer)
        throws IOException {
        int capacity = context.capacity;
        int keySize = context.keySize;
        int payloadSize = context.payloadSize;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for available slot
            i = (++i) % k, j++) { // wraps around table

            long ai = index(i, context.entrySize);
            byte currentMode = read(filer, ai);
            if (currentMode == cNull || currentMode == cSkip) {
                if (context.count >= context.maxCount) {
                    throw new OverCapacityException(context.count + " > " + context.maxCount + " ? " + context.requested);
                }
                write(filer, ai, mode);
                write(filer, (ai + 1), 0, context.keyLengthSize, key, keySize, keyOffset, stackBuffer);
                write(filer, (ai + 1 + context.keyLengthSize + keySize), 0, context.payloadLengthSize, payload, payloadSize, _payloadOffset,
                    stackBuffer);
                setCount(context, filer, context.count + 1, stackBuffer);
                return i;
            }
            if (equals(filer, ai, context.keyLengthSize, key.length, key, keyOffset, stackBuffer)) {
                write(filer, ai, mode);
                write(filer, (ai + 1 + context.keyLengthSize + keySize), 0, context.payloadLengthSize, payload, payloadSize, _payloadOffset,
                    stackBuffer);
                return i;
            }
        }
        return -1;
    }

    private void write(Filer filer, long offset, int destOffset, int length, byte[] key, int size, int keyOffset, StackBuffer stackBuffer) throws IOException {

        if (length == 0) {
        } else if (length == 1) {
            write(filer, offset + destOffset, (byte) key.length);
        } else if (length == 2) {
            writeUnsignedShort(filer, offset + destOffset, key.length, stackBuffer);
        } else if (length == 4) {
            writeInt(filer, offset + destOffset, key.length, stackBuffer);
        } else {
            throw new RuntimeException("Unsupported length. 0,1,2,4 valid but encounterd:" + length);
        }
        write(filer, offset + destOffset + length, key, keyOffset, key.length);

        int padding = size - destOffset - key.length;
        if (padding > 0) {
            write(filer, offset + destOffset + length + key.length, new byte[padding], 0, padding);
        }
    }

    public boolean contains(Filer filer, MapContext context, byte[] _key, StackBuffer stackBuffer) throws IOException {
        return get(filer, context, _key, stackBuffer) != -1;
    }

    public long startOfKey(long setIndex, int entrySize) {
        return (index(setIndex, entrySize) + 1); //  +1 to skip mode
    }

    public byte[] getKeyAtIndex(Filer filer, MapContext context, long i, StackBuffer stackBuffer) throws IOException {
        long ai = index(i, context.entrySize);
        byte mode = read(filer, ai);
        if (mode == cSkip || mode == cNull) {
            return null;
        }
        return getKey(filer, context, i, stackBuffer);
    }

    public long startOfPayload(long setIndex, int entrySize, int keyLength, int keySize) {
        long ai = index(setIndex, entrySize);
        return (ai + 1 + keyLength + keySize);
    }

    public byte[] getPayloadAtIndex(Filer filer, MapContext context, int i, StackBuffer stackBuffer) throws IOException {
        if (i < 0 || i >= context.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(filer, stackBuffer) - 1) + ")");
        }
        long ai = index(i, context.entrySize);
        byte mode = read(filer, ai);
        if (mode == cSkip || mode == cNull) {
            return null;
        }
        return getPayload(filer, context, i, stackBuffer);
    }

    public void setPayloadAtIndex(Filer filer, MapContext context, long i, int _destOffset, byte[] payload, int _poffset, int _plength, StackBuffer stackBuffer)
        throws IOException {
        if (i < 0 || i >= context.capacity) {
            throw new RuntimeException("Requested index (" + i + ") is out of bounds (0->" + (getCapacity(filer, stackBuffer) - 1) + ")");
        }
        long ai = index(i, context.entrySize);
        byte mode = read(filer, ai);
        if (mode == cSkip || mode == cNull) {
            return;
        }
        long offset = (ai + 1 + context.keyLengthSize + context.keySize);
        write(filer, offset, _destOffset, context.payloadLengthSize, payload, context.payloadSize, _poffset, stackBuffer);
    }

    public byte[] getPayload(Filer filer, MapContext context, byte[] key, StackBuffer stackBuffer) throws IOException {
        long i = get(filer, context, key, stackBuffer);
        return (i == -1) ? null : getPayload(filer, context, i, stackBuffer);
    }

    public byte[] getPayload(Filer filer, MapContext context, long keyHash, byte[] key, StackBuffer stackBuffer) throws IOException {
        long i = get(filer, context, keyHash, key, stackBuffer);
        return (i == -1) ? null : getPayload(filer, context, i, stackBuffer);
    }

    public long get(Filer filer, MapContext context, byte[] key, StackBuffer stackBuffer) throws IOException {
        return get(filer, context, key, 0, stackBuffer);
    }

    public long get(Filer filer, MapContext context, long keyHash, byte[] key, StackBuffer stackBuffer) throws IOException {
        return get(filer, context, keyHash, key, 0, stackBuffer);
    }

    public long get(Filer filer, MapContext context, byte[] key, int keyOffset, StackBuffer stackBuffer) throws IOException {
        return get(filer, context, hash(key, keyOffset, key.length), key, keyOffset, stackBuffer);
    }

    public long get(Filer filer, MapContext context, long keyHash, byte[] key, int keyOffset, StackBuffer stackBuffer) throws IOException {
        if (key == null || key.length == 0) {
            return -1;
        }
        int entrySize = context.entrySize;
        int capacity = context.capacity;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            long ai = index(i, entrySize);
            byte mode = read(filer, ai);
            if (mode == cSkip) {
                continue;
            }
            if (mode == cNull) {
                return -1;
            }
            if (equals(filer, ai, context.keyLengthSize, key.length, key, keyOffset, stackBuffer)) {
                return i;
            }
        }
        return -1;
    }

    public byte getMode(Filer filer, MapContext context, long i) throws IOException {
        long ai = index(i, context.entrySize);
        return read(filer, ai);
    }

    public byte[] getKey(Filer filer, MapContext context, long i, StackBuffer stackBuffer) throws IOException {
        long ai = index(i, context.entrySize);
        int length = length(filer, context.keyLengthSize, context.keySize, ai + 1, stackBuffer);
        byte[] k = new byte[length];
        read(filer, ai + 1 + context.keyLengthSize, k, 0, length);
        return k;
    }

    private int length(Filer filer, byte lengthSize, int size, long i, StackBuffer stackBuffer) throws IOException {
        if (lengthSize == 0) {
            return size;
        } else if (lengthSize == 1) {
            return read(filer, i);
        } else if (lengthSize == 2) {
            return readUnsignedShort(filer, i, stackBuffer);
        } else {
            return readInt(filer, i, stackBuffer);
        }
    }

    public byte[] getPayload(Filer filer, MapContext context, long i, StackBuffer stackBuffer) throws IOException {
        long ai = index(i, context.entrySize);
        long offest = ai + 1 + context.keyLengthSize + context.keySize;
        int length = length(filer, context.payloadLengthSize, context.payloadSize, offest, stackBuffer);
        byte[] p = new byte[length];
        read(filer, offest + context.payloadLengthSize, p, 0, length);
        return p;
    }

    public long remove(Filer filer, MapContext context, byte[] key, StackBuffer stackBuffer) throws IOException {
        return remove(filer, context, key, 0, stackBuffer);
    }

    public long remove(Filer filer, MapContext context, long keyHash, byte[] key, StackBuffer stackBuffer) throws IOException {
        return remove(filer, context, keyHash, key, 0, stackBuffer);
    }

    public long remove(Filer filer, MapContext context, byte[] key, int keyOffset, StackBuffer stackBuffer) throws IOException {
        return remove(filer, context, hash(key, 0, key.length), key, keyOffset, stackBuffer);
    }

    public long remove(Filer filer, MapContext context, long keyHash, byte[] key, int keyOffset, StackBuffer stackBuffer) throws IOException {
        if (key == null || key.length == 0) {
            return -1;
        }
        int capacity = context.capacity;
        int entrySize = context.entrySize;
        for (long i = keyHash % (capacity - 1), j = 0, k = capacity; // stack vars for efficiency
            j < k; // max search for key
            i = (++i) % k, j++) { // wraps around table

            long ai = index(i, context.entrySize);
            byte mode = read(filer, ai);
            if (mode == cSkip) {
                continue;
            }
            if (mode == cNull) {
                return -1;
            }
            if (equals(filer, ai, context.keyLengthSize, key.length, key, keyOffset, stackBuffer)) {
                long next = (i + 1) % k;
                if (read(filer, index(next, entrySize)) == cNull) {
                    for (long z = i; z >= 0; z--) {
                        if (read(filer, index(z, entrySize)) != cSkip) {
                            break;
                        }
                        write(filer, index(z, entrySize), cNull);
                    }
                    write(filer, index(i, entrySize), cNull);
                } else {
                    write(filer, index(i, entrySize), cSkip);
                }
                setCount(context, filer, context.count - 1, stackBuffer);
                return i;
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
                if (read(filer, ai) == cNull) {
                    continue;
                }
                if (read(filer, ai) == cSkip) {
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
        CopyToStream stream,
        StackBuffer stackBuffer) throws IOException {

        int fcapacity = fromContext.capacity;
        int fkeySize = fromContext.keySize;
        int fpayloadSize = fromContext.payloadSize;
        long fcount = fromContext.count;

        int tkeySize = toContext.keySize;
        int tpayloadSize = toContext.payloadSize;
        long tcount = toContext.count;
        int tmaxCount = getMaxCount(toFiler, stackBuffer);

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
            byte mode = read(fromFiler, ai);
            if (mode == cNull) {
                continue;
            }
            if (mode == cSkip) {
                continue;
            }
            fcount--;
            byte[] key = getKey(fromFiler, fromContext, fromIndex, stackBuffer);
            byte[] payload = getPayload(fromFiler, fromContext, fromIndex, stackBuffer);
            long toIndex = add(toFiler, toContext, mode, key, payload, stackBuffer);

            if (stream != null) {
                stream.copied(fromIndex, toIndex);
            }

            if (fcount < 0) {
                break;
            }
        }
    }

    public interface CopyToStream {

        void copied(long fromIndex, long toIndex);
    }

    public void toSysOut(Filer filer, MapContext context, StackBuffer stackBuffer) throws IOException {
        try {
            int capacity = context.capacity;
            for (int i = 0; i < capacity; i++) {
                long ai = index(i, context.entrySize);
                if (read(filer, ai) == cNull) {
                    System.out.println("\t" + i + "): null");
                    continue;
                }
                if (read(filer, ai) == cSkip) {
                    System.out.println("\t" + i + "): skip");
                    continue;
                }
                System.out.println("\t" + i + "): "
                    + Arrays.toString(getKey(filer, context, i, stackBuffer)) + "->"
                    + Arrays.toString(getPayload(filer, context, i, stackBuffer)));
            }
        } catch (Exception x) {
        }
    }

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
        return hash == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(hash);
    }

    public boolean stream(final Filer filer, final MapContext context, final Object lock, EntryStream stream, StackBuffer stackBuffer) throws IOException,
        InterruptedException {
        for (int index = 0; index < context.capacity; index++) {
            byte[] key;
            byte[] payload = null;
            synchronized (lock) {
                key = getKeyAtIndex(filer, context, index, stackBuffer);
                if (key != null) {
                    payload = getPayloadAtIndex(filer, context, index, stackBuffer);
                }
            }
            if (payload != null) {
                if (!stream.stream(new Entry(key, payload, index))) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean streamKeys(final Filer filer, final MapContext context, final Object lock, KeyStream stream, StackBuffer stackBuffer) throws IOException,
        InterruptedException {
        for (int index = 0; index < context.capacity; index++) {
            byte[] key;
            synchronized (lock) {
                key = getKeyAtIndex(filer, context, index, stackBuffer);
            }
            if (key != null) {
                if (!stream.stream(key)) {
                    return false;
                }
            }
        }
        return true;
    }

    public interface EntryStream {

        boolean stream(Entry entry) throws IOException, InterruptedException;
    }

    public interface KeyStream {

        boolean stream(byte[] key) throws IOException, InterruptedException;
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

    private boolean equals(Filer filer, long start, int keyLength, int keySize, byte[] b, int boffset, StackBuffer stackBuffer) throws IOException {
        start++; // remove mode byte
        if (keyLength == 0) {
        } else if (keyLength == 1) {
            if (read(filer, start) != keySize) {
                return false;
            }
            start++;
        } else if (keyLength == 2) {
            if (readUnsignedShort(filer, start, stackBuffer) != keySize) {
                return false;
            }
            start += 2;
        } else if (keyLength == 4) {
            if (readInt(filer, start, stackBuffer) != keySize) {
                return false;
            }
            start += 4;
        } else {
            throw new RuntimeException("Unsupported keylength=" + keyLength);
        }
        byte[] currentKey = new byte[keySize];
        filer.seek(start);
        filer.read(currentKey);
        for (int i = 0; i < keySize; i++) {
            if (currentKey[i] != b[boffset + i]) {
                return false;
            }
        }
        return true;
    }

    byte read(Filer filer, long start) throws IOException {
        filer.seek(start);
        return (byte) filer.read();
    }

    void write(Filer filer, long start, byte v) throws IOException {
        filer.seek(start);
        filer.write(v);
    }

    int readShort(Filer filer, long start, StackBuffer stackBuffer) throws IOException {
        filer.seek(start);
        byte[] bytes = new byte[2];
        filer.read(bytes);
        short v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    int readUnsignedShort(Filer filer, long start, StackBuffer stackBuffer) throws IOException {
        filer.seek(start);
        byte[] bytes = new byte[2];
        filer.read(bytes);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    int readInt(Filer filer, long start, StackBuffer stackBuffer) throws IOException {
        filer.seek(start);
        filer.read(stackBuffer.primitiveBuffer, 0, 4);
        int v = 0;
        v |= (stackBuffer.primitiveBuffer[0] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[1] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[2] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[3] & 0xFF);
        return v;
    }

    float readFloat(Filer filer, long start, StackBuffer stackBuffer) throws IOException {
        filer.seek(start);
        filer.read(stackBuffer.primitiveBuffer, 0, 4);
        int v = 0;
        v |= (stackBuffer.primitiveBuffer[0] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[1] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[2] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[3] & 0xFF);
        return Float.intBitsToFloat(v);
    }

    long readLong(Filer filer, long start, StackBuffer stackBuffer) throws IOException {
        filer.seek(start);
        filer.read(stackBuffer.primitiveBuffer, 0, 8);
        long v = 0;
        v |= (stackBuffer.primitiveBuffer[0] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[1] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[2] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[3] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[4] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[5] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[6] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[7] & 0xFF);
        return v;
    }

    double readDouble(Filer filer, long start, StackBuffer stackBuffer) throws IOException {
        filer.seek(start);
        filer.read(stackBuffer.primitiveBuffer, 0, 8);
        long v = 0;
        v |= (stackBuffer.primitiveBuffer[0] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[1] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[2] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[3] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[4] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[5] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[6] & 0xFF);
        v <<= 8;
        v |= (stackBuffer.primitiveBuffer[7] & 0xFF);
        return Double.longBitsToDouble(v);
    }

    void writeUnsignedShort(Filer filer, long start, int v, StackBuffer stackBuffer) throws IOException {
        filer.seek(start);
        stackBuffer.primitiveBuffer[0] = (byte) (v >>> 8);
        stackBuffer.primitiveBuffer[1] = (byte) (v);
        filer.write(stackBuffer.primitiveBuffer, 0, 2);
    }

    void writeInt(Filer filer, long start, int v, StackBuffer stackBuffer) throws IOException {
        filer.seek(start);
        stackBuffer.primitiveBuffer[0] = (byte) (v >>> 24);
        stackBuffer.primitiveBuffer[1] = (byte) (v >>> 16);
        stackBuffer.primitiveBuffer[2] = (byte) (v >>> 8);
        stackBuffer.primitiveBuffer[3] = (byte) (v);

        filer.write(stackBuffer.primitiveBuffer, 0, 4);
    }

    void read(Filer filer, long start, byte[] read, int offset, int length) throws IOException {
        filer.seek(start);
        filer.read(read, offset, length);
    }

    void write(Filer filer, long start, byte[] towrite, int offest, int length) throws IOException {
        filer.seek(start);
        filer.write(towrite, offest, length);
    }

}
