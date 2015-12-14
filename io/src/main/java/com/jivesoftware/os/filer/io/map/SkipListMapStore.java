package com.jivesoftware.os.filer.io.map;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkMetrics;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This is a skip list which is backed by a byte[]. This collection wastes quite a bit of space in favor of page in and out speed. May make sense to use a
 * compression strategy when sending over the wire. Each entry you sadd cost a fixed amount of space. This can be calculate by the following: entrySize =
 * entrySize+(1+(entrySize*maxColumHeight); maxColumHeight = ? where 2 ^ ? is > maxCount
 * <p>
 * The key composed of all BYTE.MIN_VALUE is typically reserved as the head of the list.
 *
 * @author jonathan
 */
public class SkipListMapStore {

    static public final SkipListMapStore INSTANCE = new SkipListMapStore();

    private static final int maxChunkPower = 32;
    private static ChunkMetrics.ChunkMetric[] openCounts = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] createCounts = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] addCounts = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] addHits = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] addSeeks = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] addInserts = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] addAppends = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] copyToCounts = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] copyToTimes = new ChunkMetrics.ChunkMetric[maxChunkPower];

    static {
        for (int i = 0; i < maxChunkPower; i++) {
            String size = "2_pow_" + (i > 9 ? i : "0" + i);
            openCounts[i] = ChunkMetrics.get("SkipListMapStore", size, "openCounts");
            createCounts[i] = ChunkMetrics.get("SkipListMapStore", size, "createCounts");
            addCounts[i] = ChunkMetrics.get("SkipListMapStore", size, "addCounts");
            addHits[i] = ChunkMetrics.get("SkipListMapStore", size, "addHits");
            addSeeks[i] = ChunkMetrics.get("SkipListMapStore", size, "addSeeks");
            addInserts[i] = ChunkMetrics.get("SkipListMapStore", size, "addInserts");
            addAppends[i] = ChunkMetrics.get("SkipListMapStore", size, "addAppends");
            copyToCounts[i] = ChunkMetrics.get("SkipListMapStore", size, "copyToCounts");
            copyToTimes[i] = ChunkMetrics.get("SkipListMapStore", size, "copyToTimes");
        }
    }

    private SkipListMapStore() {
    }

    private static final int cColumKeySize = 4; // stores the int index of the key it points to !! could make dynamic to save space

    public long computeFilerSize(
        int maxCount,
        int keySize,
        boolean variableKeySizes,
        int payloadSize) throws IOException {
        maxCount += 2; // room for a head key and a tail key
        byte maxColumnHeight = maxColumnHeight(maxCount, MapStore.cMapVersion);
        payloadSize = payloadSize(maxColumnHeight) + payloadSize;
        return MapStore.INSTANCE.computeFilerSize(maxCount, keySize, variableKeySizes, payloadSize, false);
    }

    byte maxColumnHeight(int maxCount, int version) {
        if (version < 3) {
            return (byte) 9;
        } else if (version == 3) {
            return (byte) Math.max(8, Math.min(32, FilerIO.chunkLength(maxCount))); // lol
        } else {
            int capacity = MapStore.calculateCapacity(maxCount);
            return (byte) Math.max(8, Math.min(32, FilerIO.chunkPower(capacity, 0)));
        }
    }

    int payloadSize(byte maxHeight) {
        return 1 + (cColumKeySize * maxHeight);
    }

    public SkipListMapContext create(
        int _maxCount,
        byte[] headKey,
        int keySize,
        boolean variableKeySizes,
        int _payloadSize,
        //byte maxColumnHeight,
        SkipListComparator _valueComparator,
        Filer filer,
        StackBuffer stackBuffer) throws IOException {
        if (headKey.length != keySize) {
            throw new RuntimeException("Expected that headKey.length == keySize");
        }
        _maxCount += 2;
        byte maxColumnHeight = maxColumnHeight(_maxCount, MapStore.cMapVersion);
        int columnAndPayload = payloadSize(maxColumnHeight) + _payloadSize;
        MapContext mapContext = MapStore.INSTANCE.create(_maxCount, keySize, variableKeySizes, columnAndPayload, false, filer, stackBuffer);
        int headKeyIndex = (int) MapStore.INSTANCE.add(filer, mapContext, (byte) 1, headKey,
            newColumn(new byte[_payloadSize], maxColumnHeight, maxColumnHeight), stackBuffer);
        SkipListMapContext context = new SkipListMapContext(mapContext, maxColumnHeight, headKeyIndex, headKey, _valueComparator);

        int power = FilerIO.chunkPower(context.mapContext.capacity, 0);
        createCounts[power].inc(1);

        return context;
    }

    public SkipListMapContext open(byte[] headKey,
        SkipListComparator _valueComparator,
        Filer filer,
        StackBuffer stackBuffer) throws IOException {

        MapContext mapContext = MapStore.INSTANCE.open(filer, stackBuffer);
        int headKeyIndex = (int) MapStore.INSTANCE.get(filer, mapContext, headKey, stackBuffer);
        if (headKeyIndex == -1) {
            throw new RuntimeException("SkipListSetPage:Invalid Page!");
        }
        int maxCount = MapStore.INSTANCE.getMaxCount(filer, stackBuffer);
        int version = MapStore.INSTANCE.getMapVersion(filer);
        byte maxColumnHeight = maxColumnHeight(maxCount, version);
        SkipListMapContext context = new SkipListMapContext(mapContext, maxColumnHeight, headKeyIndex, headKey, _valueComparator);

        int power = FilerIO.chunkPower(context.mapContext.capacity, 0);
        openCounts[power].inc(1);

        return context;
    }

    public long getCount(Filer filer, SkipListMapContext page, StackBuffer stackBuffer) throws IOException {
        return MapStore.INSTANCE.getCount(filer, stackBuffer) - 1; // -1 because of head
    }

    public long add(Filer filer, SkipListMapContext context, byte[] key, byte[] _payload, StackBuffer stackBuffer) throws IOException {

        int power = FilerIO.chunkPower(context.mapContext.capacity, 0);
        addCounts[power].inc(1);

        int index = (int) MapStore.INSTANCE.get(filer, context.mapContext, key, stackBuffer);
        if (index != -1) { // aready exists so just update payload
            MapStore.INSTANCE.setPayloadAtIndex(filer, context.mapContext, index, columnSize(context.maxHeight), _payload, 0, _payload.length, stackBuffer);
            addHits[power].inc(1);
            return index;
        }

        byte[] newColumn = newColumn(_payload, context.maxHeight, (byte) -1); // create a new colum for a new key
        final int insertsIndex = (int) MapStore.INSTANCE.add(filer, context.mapContext, (byte) 1, key, newColumn, stackBuffer);

        int level = context.maxHeight - 1;
        int ilevel = columnLength(filer, context, insertsIndex);
        int atIndex = context.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(filer, context, atIndex, level, stackBuffer);
            if (nextIndex == -1) {
                addAppends[power].inc(1);
                if (level < ilevel) {
                    wcolumnLevel(filer, context, atIndex, level, insertsIndex, stackBuffer);
                    if (level == 1) {
                        wcolumnLevel(filer, context, insertsIndex, 0, atIndex, stackBuffer);
                    }
                }
                level--;
            } else {
                int compare = context.keyComparator.compare(MapStore.INSTANCE.getKey(filer, context.mapContext, nextIndex, stackBuffer),
                    MapStore.INSTANCE.getKey(filer, context.mapContext, insertsIndex, stackBuffer));
                if (compare == 0) {
                    throw new RuntimeException("should be impossible");
                } else if (compare < 0) { // keep looking forward
                    addSeeks[power].inc(1);
                    atIndex = nextIndex;
                } else { // insert
                    addInserts[power].inc(1);
                    if (level < ilevel) {
                        wcolumnLevel(filer, context, insertsIndex, level, nextIndex, stackBuffer);
                        wcolumnLevel(filer, context, atIndex, level, insertsIndex, stackBuffer);
                        if (level == 1) {
                            wcolumnLevel(filer, context, insertsIndex, 0, atIndex, stackBuffer);
                            wcolumnLevel(filer, context, nextIndex, 0, insertsIndex, stackBuffer);
                        }
                    }
                    level--;
                }
            }
        }
        return insertsIndex;
    }

    public byte[] findWouldInsertAtOrAfter(Filer filer, SkipListMapContext context, byte[] _key, StackBuffer stackBuffer) throws IOException {

        int index = (int) MapStore.INSTANCE.get(filer, context.mapContext, _key, stackBuffer);
        if (index != -1) { // aready exists so return self
            return _key;
        }
        // create a new colum for a new key

        int level = context.maxHeight - 1;
        int atIndex = context.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(filer, context, atIndex, level, stackBuffer);
            if (nextIndex == -1) {
                if (level == 1) {
                    return MapStore.INSTANCE.getKeyAtIndex(filer, context.mapContext, atIndex, stackBuffer);
                }
                level--;
            } else {
                int compare = context.keyComparator.compare(MapStore.INSTANCE.getKey(filer, context.mapContext, nextIndex, stackBuffer),
                    _key);
                if (compare == 0) {
                    throw new RuntimeException("should be impossible");
                } else if (compare < 0) { // keep looking forward
                    atIndex = nextIndex;
                } else { // insert
                    if (level == 1) {
                        if (atIndex == context.headIndex) {
                            return null;
                        }
                        return MapStore.INSTANCE.getKeyAtIndex(filer, context.mapContext, atIndex, stackBuffer);
                    }
                    level--;
                }
            }
        }
        return null;
    }

    public byte[] getFirst(Filer filer, SkipListMapContext page, StackBuffer stackBuffer) throws IOException {
        int firstIndex = rcolumnLevel(filer, page, page.headIndex, 1, stackBuffer);
        if (firstIndex == -1) {
            return null;
        } else {
            return MapStore.INSTANCE.getKeyAtIndex(filer, page.mapContext, firstIndex, stackBuffer);
        }
    }

    public void remove(Filer filer, SkipListMapContext context, byte[] _key, StackBuffer stackBuffer) throws IOException {
        int removeIndex = (int) MapStore.INSTANCE.get(filer, context.mapContext, _key, stackBuffer);
        if (removeIndex == -1) { // doesn't exists so return
            return;
        }

        int level = context.maxHeight - 1;
        int atIndex = context.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(filer, context, atIndex, level, stackBuffer);
            if (nextIndex == -1) {
                level--;
            } else {
                int compare = context.keyComparator.compare(MapStore.INSTANCE.getKey(filer, context.mapContext, nextIndex, stackBuffer),
                    MapStore.INSTANCE.getKey(filer, context.mapContext, removeIndex, stackBuffer));
                if (compare == 0) {
                    while (level > -1) {
                        int removesNextIndex = rcolumnLevel(filer, context, removeIndex, level, stackBuffer);
                        wcolumnLevel(filer, context, atIndex, level, removesNextIndex, stackBuffer);
                        if (level == 0) {
                            wcolumnLevel(filer, context, removesNextIndex, level, atIndex, stackBuffer);
                        }
                        level--;
                    }

                } else if (compare < 0) {
                    atIndex = nextIndex;
                } else {
                    level--;
                }
            }
        }
        MapStore.INSTANCE.remove(filer, context.mapContext, _key, stackBuffer);
    }

    public byte[] getPrior(Filer filer, SkipListMapContext context, byte[] key, StackBuffer stackBuffer) throws IOException {
        int index = (int) MapStore.INSTANCE.get(filer, context.mapContext, key, stackBuffer);
        if (index == -1) {
            return null;
        } else {
            int pi = rcolumnLevel(filer, context, index, 0, stackBuffer);
            if (pi == -1) {
                return null;
            } else {
                byte[] got = MapStore.INSTANCE.getKeyAtIndex(filer, context.mapContext, pi, stackBuffer);
                if (Arrays.equals(context.headKey, got)) {
                    return null; // don't give out head key
                }
                return got;
            }
        }
    }

    public byte[] getNextKey(Filer filer, SkipListMapContext context, byte[] key, StackBuffer stackBuffer) throws IOException {
        int index = (int) MapStore.INSTANCE.get(filer, context.mapContext, key, stackBuffer);
        if (index == -1) {
            return null;
        } else {
            int nextIndex = rcolumnLevel(filer, context, index, 1, stackBuffer);
            if (nextIndex == -1) {
                return null;
            } else {
                return MapStore.INSTANCE.getKeyAtIndex(filer, context.mapContext, nextIndex, stackBuffer);
            }
        }
    }

    public byte[] getExistingPayload(Filer filer, SkipListMapContext context, byte[] key, StackBuffer stackBuffer) throws IOException {
        int index = (int) MapStore.INSTANCE.get(filer, context.mapContext, key, stackBuffer);
        if (index == -1) {
            return null;
        } else {
            return getColumnPayload(filer, context, index, context.maxHeight);
        }
    }

    public boolean streamKeys(final Filer filer, final SkipListMapContext context, final Object lock,
        List<KeyRange> ranges, MapStore.KeyStream stream, StackBuffer stackBuffer) throws IOException, InterruptedException {
        if (ranges == null) {
            for (int index = 0; index < context.mapContext.capacity; index++) {
                if (index == context.headIndex) { // Barf
                    continue;
                }
                byte[] key;
                synchronized (lock) {
                    key = MapStore.INSTANCE.getKeyAtIndex(filer, context.mapContext, index, stackBuffer);
                }
                if (key != null) {
                    if (!stream.stream(key)) {
                        return false;
                    }
                }
            }

        } else {
            for (KeyRange range : ranges) {
                byte[] key = findWouldInsertAtOrAfter(filer, context, range.getStartInclusiveKey(), stackBuffer);
                if (key == null) {
                    key = getNextKey(filer, context, context.headKey, stackBuffer);
                }
                if (key != null) {
                    if (range.contains(key)) {
                        if (!stream.stream(key)) {
                            return false;
                        }
                    }
                    byte[] next = getNextKey(filer, context, key, stackBuffer);
                    while (next != null && range.contains(next)) {
                        if (!stream.stream(next)) {
                            return false;
                        }
                        next = getNextKey(filer, context, next, stackBuffer);
                    }
                }
            }

        }
        return true;
    }

    /**
     * this is a the lazy impl... this can be highly optimized when we have time!
     */
    public void copyTo(Filer f, SkipListMapContext from, final Filer t, final SkipListMapContext to, MapStore.CopyToStream stream, StackBuffer stackBuffer)
        throws IOException {
        int power = FilerIO.chunkPower(to.mapContext.capacity, 0);
        long start = System.currentTimeMillis();
        for (int fromIndex = 0; fromIndex < from.mapContext.capacity; fromIndex++) {
            if (fromIndex == from.headIndex) { // Barf
                continue;
            }

            long ai = MapStore.INSTANCE.index(fromIndex, from.mapContext.entrySize);
            byte mode = MapStore.INSTANCE.read(f, (int) ai);
            if (mode == MapStore.cNull) {
                continue;
            }
            if (mode == MapStore.cSkip) {
                continue;
            }

            byte[] key = MapStore.INSTANCE.getKey(f, from.mapContext, fromIndex, stackBuffer);
            byte[] payload = getColumnPayload(f, from, fromIndex, from.maxHeight);
            long toIndex = add(t, to, key, payload, stackBuffer);

            if (stream != null) {
                stream.copied(fromIndex, toIndex);
            }
        }
        copyToCounts[power].inc(1);
        copyToTimes[power].inc(System.currentTimeMillis() - start);
    }

    private byte[] newColumn(byte[] _payload, int _maxHeight, byte _height) {
        if (_height <= 0) {
            byte newH = 2;
            while (Math.random() > 0.5d) { // could pick a rand number bewteen 1 and 32 instead
                if (newH + 1 >= _maxHeight) {
                    break;
                }
                newH++;
            }
            _height = newH;
        }
        byte[] column = new byte[1 + (_maxHeight * cColumKeySize) + _payload.length];
        column[0] = _height;
        for (int i = 0; i < _maxHeight; i++) {
            setColumKey(column, i, FilerIO.intBytes(-1)); // fill with nulls ie -1
        }
        System.arraycopy(_payload, 0, column, 1 + (_maxHeight * cColumKeySize), _payload.length);
        return column;
    }

    private void setColumKey(byte[] _column, int _h, byte[] _key) {
        System.arraycopy(_key, 0, _column, 1 + (_h * cColumKeySize), cColumKeySize);
    }

    private byte columnLength(Filer f, SkipListMapContext context, int setIndex) throws IOException {
        int entrySize = context.mapContext.entrySize;
        int keyLength = context.mapContext.keyLengthSize;
        int keySize = context.mapContext.keySize;
        return MapStore.INSTANCE.read(f, MapStore.INSTANCE.startOfPayload(setIndex, entrySize, keyLength, keySize));
    }

    private int rcolumnLevel(Filer f, SkipListMapContext context, int setIndex, int level, StackBuffer stackBuffer) throws IOException {
        int entrySize = context.mapContext.entrySize;
        int keyLength = context.mapContext.keyLengthSize;
        int keySize = context.mapContext.keySize;
        int offset = (int) MapStore.INSTANCE.startOfPayload(setIndex, entrySize, keyLength, keySize) + 1 + (level * cColumKeySize);
        return MapStore.INSTANCE.readInt(f, offset, stackBuffer);
    }

    private void wcolumnLevel(Filer f, SkipListMapContext context, int setIndex, int level, int v, StackBuffer stackBuffer) throws IOException {
        int entrySize = context.mapContext.entrySize;
        int keyLength = context.mapContext.keyLengthSize;
        int keySize = context.mapContext.keySize;
        int offset = (int) MapStore.INSTANCE.startOfPayload(setIndex, entrySize, keyLength, keySize) + 1 + (level * cColumKeySize);
        MapStore.INSTANCE.writeInt(f, offset, v, stackBuffer);
    }

    private int columnSize(int maxHeight) {
        return 1 + (cColumKeySize * maxHeight);
    }

    private byte[] getColumnPayload(Filer f, SkipListMapContext context, int setIndex, int maxHeight) throws IOException {
        int entrySize = context.mapContext.entrySize;
        int keyLength = context.mapContext.keyLengthSize;
        int keySize = context.mapContext.keySize;
        int startOfPayload = (int) MapStore.INSTANCE.startOfPayload(setIndex, entrySize, keyLength, keySize);
        int size = context.mapContext.payloadSize - columnSize(maxHeight);
        byte[] payload = new byte[size];
        MapStore.INSTANCE.read(f, startOfPayload + 1 + (maxHeight * cColumKeySize), payload, 0, size);
        return payload;
    }

    public void toSysOut(Filer f, SkipListMapContext context, BytesToString keyToString, StackBuffer stackBuffer) throws IOException {
        if (keyToString == null) {
            keyToString = new BytesToBytesString();
        }
        int atIndex = context.headIndex;
        int count = 0;
        while (atIndex != -1) {
            toSysOut(f, context, atIndex, keyToString, stackBuffer);
            atIndex = rcolumnLevel(f, context, atIndex, 1, stackBuffer);

            if (count > MapStore.INSTANCE.getCount(f, stackBuffer)) {
                System.out.println("BAD Panda! Cyclic");
                break;
            }
            count++;
        }
    }

    /**
     *
     */
    static public interface BytesToString {

        /**
         * @param bytes
         * @return
         */
        String bytesToString(byte[] bytes);
    }

    /**
     *
     */
    static public class BytesToDoubleString implements BytesToString {

        /**
         * @param bytes
         * @return
         */
        @Override
        public String bytesToString(byte[] bytes) {
            return Double.toString(FilerIO.bytesDouble(bytes));
        }
    }

    /**
     *
     */
    static public class BytesToBytesString implements BytesToString {

        /**
         * @param bytes
         * @return
         */
        @Override
        public String bytesToString(byte[] bytes) {
            return Arrays.toString(bytes);
        }
    }

    private void toSysOut(Filer filer, SkipListMapContext context, int index, BytesToString keyToString, StackBuffer stackBuffer) throws IOException {
        byte[] key = MapStore.INSTANCE.getKeyAtIndex(filer, context.mapContext, index, stackBuffer);
        System.out.print("\ti:" + index);
        System.out.print("\tv:" + keyToString.bytesToString(key) + " - \t");
        int l = columnLength(filer, context, index);
        for (int i = 0; i < l; i++) {
            if (i != 0) {
                System.out.print("),\t" + i + ":(");
            } else {
                System.out.print(i + ":(");
            }
            int ni = rcolumnLevel(filer, context, index, i, stackBuffer);
            if (ni == -1) {
                System.out.print("NULL");

            } else {
                byte[] nkey = MapStore.INSTANCE.getKeyAtIndex(filer, context.mapContext, ni, stackBuffer);
                if (nkey == null) {
                    System.out.print(ni + "=???");
                } else {
                    System.out.print(ni + "=" + keyToString.bytesToString(nkey));
                }

            }
        }
        System.out.println(")");
    }
}
