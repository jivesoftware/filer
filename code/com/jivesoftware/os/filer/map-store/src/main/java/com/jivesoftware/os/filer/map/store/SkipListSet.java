package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.map.store.extractors.IndexStream;
import java.io.IOException;
import java.util.Arrays;

//!! NOT FINISHED this is a starting point impl
/**
 * This is a skip list which is backed by a byte[] via a SByteArrayFixedSizeSet This collection wastes quite a bit of space infavor of page in and out speed.
 * May make sense to use a compression strategy when sending over the wire. Each entry you sadd cost a fixed amount of space. This can be calculate by the
 * following: entrySize = entrySize+(1+(entrySize*maxColumHeight); maxColumHeight = ? where 2 ^ ? is > maxCount
 *
 * The key composed of all BYTE.MIN_VALUE is reserved as the head of the list.
 *
 */

/**
 * @author jonathan
 */
public class SkipListSet {

    static final MapStore mapStore = MapStore.INSTANCE;

    private static final int cColumKeySize = 4; // stores the int index of the key it points to !! could make dynamic to save space

    public SkipListSet() {
    }

    public SkipListSetPage open(Filer filer, SkipListComparator valueComparator, byte[] headKey) throws IOException {
        MapContext chunk = mapStore.open(filer);
        byte maxHeight = heightFit(chunk.capacity);
        long headIndex = mapStore.get(filer, chunk, headKey);
        if (headIndex == -1) {
            throw new RuntimeException("SkipListSetPage:Invalid Page!");
        }
        return new SkipListSetPage(chunk, maxHeight, valueComparator, headIndex, headKey);
    }

    public SkipListSetPage create(
        int _maxCount,
        final byte[] headKey,
        final int _keySize,
        final boolean variableKeySizes,
        int _payloadSize,
        final boolean variablePayloadSizes,
        final SkipListComparator _valueComparator,
        Filer filer) throws IOException {

        if (headKey.length != _keySize) {
            throw new RuntimeException("Expected that headKey.length == keySize");
        }

        final int maxCount = _maxCount + 2;
        final byte maxHeight = heightFit(_maxCount);
        final int payloadSize = slpayloadSize(maxHeight) + _payloadSize;

        MapContext setPage = mapStore.create(maxCount, _keySize, variableKeySizes, payloadSize,
            variablePayloadSizes, filer);
        byte[] headPayload = new byte[payloadSize];
        Arrays.fill(headPayload, Byte.MIN_VALUE);
        mapStore.add(filer, setPage, (byte) 1, headKey, newColumn(headPayload, maxHeight, maxHeight));
        //stoSysOut(setPage);
        return open(filer, _valueComparator, headKey);
    }

    public int slcost(int _maxCount, int _keySize, int _payloadSize) {
        byte maxHeight = heightFit(_maxCount);
        return mapStore.cost(_maxCount, _keySize, slpayloadSize(maxHeight) + _payloadSize);
    }

    int slpayloadSize(byte maxHeight) {
        return 1 + (cColumKeySize * maxHeight);
    }

    public long slgetCount(Filer filer, SkipListSetPage page) throws IOException {
        return mapStore.getCount(filer) - 1; // -1 because of head
    }

    public void sladd(Filer filer, SkipListSetPage page, byte[] _key, byte[] _payload) throws IOException {

        long index = mapStore.get(filer, page.chunk, _key);
        if (index != -1) { // aready exists so just update payload
            mapStore.setPayloadAtIndex(filer, page.chunk, index, startOfPayload(page.maxHeight), _payload, 0, _payload.length);
            return;
        }
        int skeySize = page.chunk.keySize;
        int spayloadSize = page.chunk.payloadSize;
        // create a new colum for a new key
        byte[] newColumn = newColumn(_payload, page.maxHeight, (byte) -1);
        int insertsIndex = mapStore.add(filer, page.chunk, (byte) 1, _key, newColumn);

        int level = page.maxHeight - 1;
        int ilevel = columnLength(filer, page, insertsIndex);
        long atIndex = page.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(filer, page, atIndex, level);
            if (nextIndex == -1) {
                if (level < ilevel) {
                    wcolumnLevel(filer, page, atIndex, level, insertsIndex);
                    if (level == 1) {
                        wcolumnLevel(filer, page, insertsIndex, 0, atIndex);
                    }
                }
                level--;
            } else {
                int compare = compare(page, mapStore.startOfKey(nextIndex, page.chunk.entrySize), mapStore.startOfKey(insertsIndex, page.chunk.entrySize));
                if (compare == 0) {
                    throw new RuntimeException("should be impossible");
                } else if (compare < 0) { // keep looking forward
                    atIndex = nextIndex;
                } else { // insert
                    if (level < ilevel) {
                        wcolumnLevel(filer, page, insertsIndex, level, nextIndex);
                        wcolumnLevel(filer, page, atIndex, level, insertsIndex);
                        if (level == 1) {
                            wcolumnLevel(filer, page, insertsIndex, 0, atIndex);
                            wcolumnLevel(filer, page, nextIndex, 0, insertsIndex);
                        }
                    }
                    level--;
                }
            }
        }
    }

    /**
     * !! un-tested
     *
     * @param page
     * @param _key
     * @return
     */
    public byte[] slfindWouldInsertAfter(Filer filer, SkipListSetPage page, byte[] _key) throws IOException {

        long index = mapStore.get(filer, page.chunk, _key);
        if (index != -1) { // aready exists so return self
            return _key;
        }
        int skeySize = page.chunk.keySize;
        int spayloadSize = page.chunk.payloadSize;
        // create a new colum for a new key

        int level = page.maxHeight - 1;
        long atIndex = page.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(filer, page, atIndex, level);
            if (nextIndex == -1) {
                if (level == 1) {
                    return mapStore.getKeyAtIndex(filer, page.chunk, atIndex);
                }
                level--;
            } else {
                int compare = compare(page, mapStore.startOfKey(nextIndex, page.chunk.entrySize), _key);
                if (compare == 0) {
                    throw new RuntimeException("should be impossible");
                } else if (compare < 0) { // keep looking forward
                    atIndex = nextIndex;
                } else { // insert
                    if (level == 1) {
                        if (atIndex == page.headIndex) {
                            return null;
                        }
                        return mapStore.getKeyAtIndex(filer, page.chunk, atIndex);
                    }
                    level--;
                }
            }
        }
        return null;
    }

    /**
     * @param page
     * @return
     */
    public byte[] slgetFirst(Filer filer, SkipListSetPage page) throws IOException {
        int firstIndex = rcolumnLevel(filer, page, page.headIndex, 1);
        if (firstIndex == -1) {
            return null;
        } else {
            return mapStore.getKeyAtIndex(filer, page.chunk, firstIndex);
        }
    }

    /**
     * @param page
     * @param _key
     */
    public void slremove(Filer filer, SkipListSetPage page, byte[] _key) throws IOException {
        if (_key == null || _key.length == 0) {
            throw new RuntimeException("null not supported");
        }
        long removeIndex = mapStore.get(filer, page.chunk, _key);
        if (removeIndex == -1) { // doesn't exists so return
            return;
        }
        int skeySize = page.chunk.keySize;
        int spayloadSize = page.chunk.payloadSize;

        int level = page.maxHeight - 1;
        long atIndex = page.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(filer, page, atIndex, level);
            if (nextIndex == -1) {
                level--;
            } else {
                int compare = compare(page, mapStore.startOfKey(nextIndex, page.chunk.entrySize), mapStore.startOfKey(removeIndex, page.chunk.entrySize));
                if (compare == 0) {
                    while (level > -1) {
                        int removesNextIndex = rcolumnLevel(filer, page, removeIndex, level);
                        wcolumnLevel(filer, page, atIndex, level, removesNextIndex);
                        if (level == 0) {
                            wcolumnLevel(filer, page, removesNextIndex, level, atIndex);
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
        mapStore.remove(filer, page.chunk, _key);
    }

    /**
     * @param page
     * @param _key
     * @return
     */
    public byte[] slgetPrior(Filer filer, SkipListSetPage page, byte[] _key) throws IOException {
        if (_key == null || _key.length == 0) {
            return null;
        }
        long index = mapStore.get(filer, page.chunk, _key);
        if (index == -1) {
            return null;
        } else {
            int pi = rcolumnLevel(filer, page, index, 0);
            if (pi == -1) {
                return null;
            } else {
                byte[] got = mapStore.getKeyAtIndex(filer, page.chunk, pi);
                if (isHeadKey(page, got)) {
                    return null; // don't give out head key
                }
                return got;
            }
        }
    }

    /**
     * @param page
     * @param _key
     * @return
     */
    public byte[] slgetNext(Filer filer, SkipListSetPage page, byte[] _key) throws IOException {
        if (_key == null || _key.length == 0) {
            return null;
        }
        long index = mapStore.get(filer, page.chunk, _key);
        if (index == -1) {
            return null;
        } else {
            int nextIndex = rcolumnLevel(filer, page, index, 1);
            if (nextIndex == -1) {
                return null;
            } else {
                return mapStore.getKeyAtIndex(filer, page.chunk, nextIndex);
            }
        }
    }

    /**
     * @param page
     * @param _key
     * @return
     */
    public byte[] slgetExisting(Filer filer, SkipListSetPage page, byte[] _key) throws IOException {
        if (_key == null || _key.length == 0) {
            throw new RuntimeException("null not supported");
        }
        long index = mapStore.get(filer, page.chunk, _key);
        if (index == -1) {
            return null;
        } else {
            return getColumnPayload(filer, page, index, page.maxHeight);
        }
    }

    /**
     * @param page
     * @param key
     * @return
     */
    public byte[] slgetAfterExisting(Filer filer, SkipListSetPage page, byte[] key) throws IOException {
        if (key == null || key.length == 0) {
            throw new RuntimeException("null not supported");
        }
        long index = mapStore.get(filer, page.chunk, key);
        if (index == -1) {
            return null;
        }
        int nextIndex = rcolumnLevel(filer, page, index, 1);
        return mapStore.getKeyAtIndex(filer, page.chunk, nextIndex);
    }

    /**
     * DO NOT cache KeyPayload... It will be reused by PSkipListSet
     *
     * @param page
     * @param from inclusive
     * @param to   inclusive
     * @param _max
     * @param _get
     * @throws Exception
     */
    public void slgetSlice(Filer filer, SkipListSetPage page, byte[] from, byte[] to, int _max, IndexStream<Exception> _get) throws Exception {
        long at;
        if (from != null && from.length > 0) {
            at = mapStore.get(filer, page.chunk, from);
            if (at == -1) {
                byte[] found = slfindWouldInsertAfter(filer, page, from);
                if (found == null) {
                    _get.stream(-1); // done because from doesn't exist
                    return;
                }
                at = mapStore.get(filer, page.chunk, found);
                at = rcolumnLevel(filer, page, at, 1); // move to next because we found the one before
            }

        } else {
            at = page.headIndex;
        }

        done:
        while (at != -1) {
            if (to != null) {
                int compare = compare(page, mapStore.startOfKey(at, page.chunk.entrySize), to);
                if (compare > 0) {
                    break;
                }
            }
            if (_get.stream(at)) {
                break;
            }
            at = rcolumnLevel(filer, page, at, 1);
            if (at == -1) {
                break;
            }
            if (_max > 0) {
                _max--;
                if (_max <= 0) {
                    break;
                }
            }
        }
        _get.stream(-1);
    }

    /**
     * this is a the lazy impl... this can be highly optimized when we have time!
     *
     * @param from
     * @param to
     * @throws Exception
     */
    public void slcopyTo(final Filer fromFiler, final SkipListSetPage from, final Filer toFiler, final SkipListSetPage to) throws Exception {
        slgetSlice(fromFiler, from, null, null, -1, new IndexStream<Exception>() {

            @Override
            public boolean stream(long ai) throws Exception {
                if (ai != -1) {
                    byte[] key = mapStore.getKeyAtIndex(fromFiler, from.chunk, ai);
                    byte[] payload = getColumnPayload(fromFiler, from, ai, from.maxHeight);
                    sladd(toFiler, to, key, payload);
                }

                return true;
            }
        });
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
        assert _height > 1 : _height < 32;
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

    private byte columnLength(Filer filer, SkipListSetPage page, long setIndex) throws IOException {
        int keyLengthSize = page.chunk.keyLengthSize;
        int keySize = page.chunk.keySize;
        int entrySize = page.chunk.entrySize;
        return mapStore.read(filer, mapStore.startOfPayload(setIndex, entrySize, keyLengthSize, keySize));
    }

    private int rcolumnLevel(Filer filer, SkipListSetPage page, long setIndex, int level) throws IOException {
        int keyLengthSize = page.chunk.keyLengthSize;
        int keySize = page.chunk.keySize;
        int entrySize = page.chunk.entrySize;
        long offset = mapStore.startOfPayload(setIndex, entrySize, keyLengthSize, keySize) + 1 + (level * cColumKeySize);
        return mapStore.readInt(filer, offset);
    }

    private void wcolumnLevel(Filer filer, SkipListSetPage page, long setIndex, int level, long v) throws IOException {
        int keyLengthSize = page.chunk.keyLengthSize;
        int keySize = page.chunk.keySize;
        int entrySize = page.chunk.entrySize;
        long offset = mapStore.startOfPayload(setIndex, entrySize, keyLengthSize, keySize) + 1 + (level * cColumKeySize);
        mapStore.writeInt(filer, offset, (int) v);
    }

    private int startOfPayload(int maxHeight) {
        return 1 + (cColumKeySize * maxHeight);
    }

    private byte[] getColumnPayload(Filer filer, SkipListSetPage page, long setIndex, int maxHeight) throws IOException {
        int keyLengthSize = page.chunk.keyLengthSize;
        int keySize = page.chunk.keySize;
        int entrySize = page.chunk.entrySize;
        long startOfPayload = mapStore.startOfPayload(setIndex, entrySize, keyLengthSize, keySize);
        int size = (page.chunk.payloadLengthSize + page.chunk.payloadSize) - startOfPayload(maxHeight);
        byte[] payload = new byte[size];
        mapStore.read(filer, (int) (startOfPayload + 1 + (maxHeight * cColumKeySize)), payload, 0, size);
        return payload;
    }

    private boolean isHeadKey(SkipListSetPage page, byte[] bytes) {
        if (bytes.length != page.headKey.length) {
            return false;
        }
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != page.headKey[i]) {
                return false;
            }
        }
        return true;
    }

    private int compare(SkipListSetPage page, int startOfAKey, int startOfBKey) throws IOException {
        return page.valueComparator.compare(page.chunk, startOfAKey, page.chunk, startOfBKey, page.chunk.keySize);
    }

    private int compare(SkipListSetPage page, int startOfAKey, byte[] _key) throws IOException {
        MapContext otherChunk = mapStore.open(new ByteArrayFiler(_key));
        return page.valueComparator.compare(page.chunk, startOfAKey, otherChunk, 0, page.chunk.keySize);
    }

    public static byte heightFit(long _length) {
        byte h = (byte) 1; // room for back links
        for (byte i = 1; i < 65; i++) { // 2^64 == long so why go anyfuther
            if (_length < Math.pow(2, i)) {
                return (byte) (h + i);
            }
        }
        return 64;
    }

    // debugging aids

    /**
     * @param page
     * @param keyToString
     */
    public void sltoSysOut(Filer filer, SkipListSetPage page, BytesToString keyToString) throws IOException {
        if (keyToString == null) {
            keyToString = new BytesToBytesString();
        }
        long atIndex = page.headIndex;
        while (atIndex != -1) {
            toSysOut(filer, page, atIndex, keyToString);
            atIndex = rcolumnLevel(filer, page, atIndex, 1);
        }
    }

    /**
     *
     */
    static abstract public class BytesToString {

        /**
         * @param bytes
         * @return
         */
        abstract public String bytesToString(byte[] bytes);
    }

    /**
     *
     */
    static public class BytesToDoubleString extends BytesToString {

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
    static public class BytesToBytesString extends BytesToString {

        /**
         * @param bytes
         * @return
         */
        @Override
        public String bytesToString(byte[] bytes) {
            return new String(bytes); // UString.toString(bytes, ",");
        }
    }

    private void toSysOut(Filer filer, SkipListSetPage page, long index, BytesToString keyToString) throws IOException {
        byte[] key = mapStore.getKeyAtIndex(filer, page.chunk, index);
        System.out.print("\t" + keyToString.bytesToString(key) + " - ");
        int l = columnLength(filer, page, index);
        for (int i = 0; i < l; i++) {
            if (i != 0) {
                System.out.print(",");

            }
            int ni = rcolumnLevel(filer, page, index, i);
            if (ni == -1) {
                System.out.print("NULL");

            } else {
                byte[] nkey = mapStore.getKeyAtIndex(filer, page.chunk, ni);
                if (nkey == null) {
                    System.out.println("??");
                } else {
                    System.out.print(keyToString.bytesToString(nkey));
                }

            }
        }
        System.out.println();
    }
}
