package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.map.store.extractors.IndexStream;
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
 *
 * @author jonathan
 */
public class SkipListSet {

    static final MapStore map = MapStore.DEFAULT;

    public SkipListSet() {
    }

    private static final int cColumKeySize = 4; // stores the int index of the key it points to !! could make dynamic to save space

    /**
     * new
     *
     * @param mapStore
     * @param id
     * @param version
     * @param _maxCount
     * @param headKey
     * @param _keySize
     * @param _payloadSize
     * @param _valueComparator
     * @param factory
     * @return
     */
    public SkipListSetPage slallocate(MapStore mapStore, byte[] id,
            long version, int _maxCount, byte[] headKey,
            int _keySize,
            boolean variableKeySizes,
            int _payloadSize,
            boolean variablePayloadSizes,
            SkipListComparator _valueComparator, ByteBufferFactory factory) {
        if (headKey.length != _keySize) {
            throw new RuntimeException("Expected that headKey.length == keySize");
        }
        _maxCount += 2;
        byte maxHeight = SkipListSetPage.heightFit(_maxCount);
        int payloadSize = slpayloadSize(maxHeight) + _payloadSize;

        MapChunk setPage = map.allocate((byte) 0, (byte) 0, id, version, _maxCount, _keySize, variableKeySizes, payloadSize, variablePayloadSizes, factory);
        byte[] headPayload = new byte[payloadSize];
        Arrays.fill(headPayload, Byte.MIN_VALUE);
        map.add(setPage, (byte) 1, headKey, newColumn(headPayload, maxHeight, maxHeight));
        //stoSysOut(setPage);
        SkipListSetPage slsPage = new SkipListSetPage(setPage, headKey, _valueComparator);
        slsPage.init(mapStore);
        return slsPage;
    }

    /**
     *
     * @param _maxCount
     * @param _keySize
     * @param _payloadSize
     * @return
     */
    public int slcost(int _maxCount, int _keySize, int _payloadSize) {
        byte maxHeight = SkipListSetPage.heightFit(_maxCount);
        return map.cost(_maxCount, _keySize, slpayloadSize(maxHeight) + _payloadSize);
    }

    /**
     *
     * @param maxHeight
     * @return
     */
    int slpayloadSize(byte maxHeight) {
        return 1 + (cColumKeySize * maxHeight);
    }

    /**
     *
     * @param page
     * @return
     */
    public long slgetCount(SkipListSetPage page) {
        return map.getCount(page.map) - 1; // -1 because of head
    }

    /**
     *
     * @param page
     * @param _key
     * @param _payload
     */
    public void sladd(SkipListSetPage page, byte[] _key, byte[] _payload) {

        long index = map.get(page.map, _key);
        if (index != -1) { // aready exists so just update payload
            map.setPayloadAtIndex(page.map, index, startOfPayload(page.maxHeight), _payload, 0, _payload.length);
            return;
        }
        int skeySize = page.map.keySize;
        int spayloadSize = page.map.payloadSize;
        // create a new colum for a new key
        byte[] newColumn = newColumn(_payload, page.maxHeight, (byte) -1);
        int insertsIndex = map.add(page.map, (byte) 1, _key, newColumn);

        int level = page.maxHeight - 1;
        int ilevel = columnLength(page, insertsIndex);
        long atIndex = page.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(page, atIndex, level);
            if (nextIndex == -1) {
                if (level < ilevel) {
                    wcolumnLevel(page, atIndex, level, insertsIndex);
                    if (level == 1) {
                        wcolumnLevel(page, insertsIndex, 0, atIndex);
                    }
                }
                level--;
            } else {
                int compare = page.compare(map.startOfKey(nextIndex, page.map.entrySize), map.startOfKey(insertsIndex, page.map.entrySize));
                if (compare == 0) {
                    throw new RuntimeException("should be impossible");
                } else if (compare < 0) { // keep looking forward
                    atIndex = nextIndex;
                } else { // insert
                    if (level < ilevel) {
                        wcolumnLevel(page, insertsIndex, level, nextIndex);
                        wcolumnLevel(page, atIndex, level, insertsIndex);
                        if (level == 1) {
                            wcolumnLevel(page, insertsIndex, 0, atIndex);
                            wcolumnLevel(page, nextIndex, 0, insertsIndex);
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
    public byte[] slfindWouldInsertAfter(SkipListSetPage page, byte[] _key) {

        long index = map.get(page.map, _key);
        if (index != -1) { // aready exists so return self
            return _key;
        }
        int skeySize = page.map.keySize;
        int spayloadSize = page.map.payloadSize;
        // create a new colum for a new key

        int level = page.maxHeight - 1;
        long atIndex = page.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(page, atIndex, level);
            if (nextIndex == -1) {
                if (level == 1) {
                    return map.getKeyAtIndex(page.map, atIndex);
                }
                level--;
            } else {
                int compare = page.compare(map.startOfKey(nextIndex, page.map.entrySize), _key);
                if (compare == 0) {
                    throw new RuntimeException("should be impossible");
                } else if (compare < 0) { // keep looking forward
                    atIndex = nextIndex;
                } else { // insert
                    if (level == 1) {
                        if (atIndex == page.headIndex) {
                            return null;
                        }
                        return map.getKeyAtIndex(page.map, atIndex);
                    }
                    level--;
                }
            }
        }
        return null;
    }

    /**
     *
     * @param page
     * @return
     */
    public byte[] slgetFirst(SkipListSetPage page) {
        int firstIndex = rcolumnLevel(page, page.headIndex, 1);
        if (firstIndex == -1) {
            return null;
        } else {
            return map.getKeyAtIndex(page.map, firstIndex);
        }
    }

    /**
     *
     * @param page
     * @param _key
     */
    public void slremove(SkipListSetPage page, byte[] _key) {
        if (_key == null || _key.length == 0) {
            throw new RuntimeException("null not supported");
        }
        long removeIndex = map.get(page.map, _key);
        if (removeIndex == -1) { // doesn't exists so return
            return;
        }
        int skeySize = page.map.keySize;
        int spayloadSize = page.map.payloadSize;

        int level = page.maxHeight - 1;
        long atIndex = page.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(page, atIndex, level);
            if (nextIndex == -1) {
                level--;
            } else {
                int compare = page.compare(map.startOfKey(nextIndex, page.map.entrySize), map.startOfKey(removeIndex, page.map.entrySize));
                if (compare == 0) {
                    while (level > -1) {
                        int removesNextIndex = rcolumnLevel(page, removeIndex, level);
                        wcolumnLevel(page, atIndex, level, removesNextIndex);
                        if (level == 0) {
                            wcolumnLevel(page, removesNextIndex, level, atIndex);
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
        map.remove(page.map, _key);
    }

    /**
     *
     * @param page
     * @param _key
     * @return
     */
    public byte[] slgetPrior(SkipListSetPage page, byte[] _key) {
        if (_key == null || _key.length == 0) {
            return null;
        }
        long index = map.get(page.map, _key);
        if (index == -1) {
            return null;
        } else {
            int pi = rcolumnLevel(page, index, 0);
            if (pi == -1) {
                return null;
            } else {
                byte[] got = map.getKeyAtIndex(page.map, pi);
                if (page.isHeadKey(got)) {
                    return null; // don't give out head key
                }
                return got;
            }
        }
    }

    /**
     *
     * @param page
     * @param _key
     * @return
     */
    public byte[] slgetNext(SkipListSetPage page, byte[] _key) {
        if (_key == null || _key.length == 0) {
            return null;
        }
        long index = map.get(page.map, _key);
        if (index == -1) {
            return null;
        } else {
            int nextIndex = rcolumnLevel(page, index, 1);
            if (nextIndex == -1) {
                return null;
            } else {
                return map.getKeyAtIndex(page.map, nextIndex);
            }
        }
    }

    /**
     *
     * @param page
     * @param _key
     * @return
     */
    public byte[] slgetExisting(SkipListSetPage page, byte[] _key) {
        if (_key == null || _key.length == 0) {
            throw new RuntimeException("null not supported");
        }
        long index = map.get(page.map, _key);
        if (index == -1) {
            return null;
        } else {
            return getColumnPayload(page, index, page.maxHeight);
        }
    }

    /**
     *
     * @param page
     * @param key
     * @return
     */
    public byte[] slgetAfterExisting(SkipListSetPage page, byte[] key) {
        if (key == null || key.length == 0) {
            throw new RuntimeException("null not supported");
        }
        long index = map.get(page.map, key);
        if (index == -1) {
            return null;
        }
        int nextIndex = rcolumnLevel(page, index, 1);
        return map.getKeyAtIndex(page.map, nextIndex);
    }

    /**
     * DO NOT cache KeyPayload... It will be reused by PSkipListSet
     *
     * @param page
     * @param from inclusive
     * @param to inclusive
     * @param _max
     * @param _get
     * @throws Exception
     */
    public void slgetSlice(SkipListSetPage page, byte[] from, byte[] to, int _max, IndexStream<Exception> _get) throws Exception {
        long at;
        if (from != null && from.length > 0) {
            at = map.get(page.map, from);
            if (at == -1) {
                byte[] found = slfindWouldInsertAfter(page, from);
                if (found == null) {
                    _get.stream(-1); // done because from doesn't exist
                    return;
                }
                at = map.get(page.map, found);
                at = rcolumnLevel(page, at, 1); // move to next because we found the one before
            }

        } else {
            at = page.headIndex;
        }

        done:
        while (at != -1) {
            if (to != null) {
                int compare = page.compare(map.startOfKey(at, page.map.entrySize), to);
                if (compare > 0) {
                    break;
                }
            }
            if (_get.stream(at)) {
                break;
            }
            at = rcolumnLevel(page, at, 1);
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
    public void slcopyTo(final SkipListSetPage from, final SkipListSetPage to) throws Exception {
        slgetSlice(from, null, null, -1, new IndexStream<Exception>() {

            @Override
            public boolean stream(long ai) throws Exception {
                if (ai != -1) {
                    byte[] key = map.getKeyAtIndex(from.map, ai);
                    byte[] payload = getColumnPayload(from, ai, from.maxHeight);
                    sladd(to, key, payload);
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

    private byte columnLength(SkipListSetPage page, long setIndex) {
        int keyLengthSize = page.map.keyLengthSize;
        int keySize = page.map.keySize;
        int entrySize = page.map.entrySize;
        return MapStore.DEFAULT.read(page.map.array, map.startOfPayload(setIndex, entrySize, keyLengthSize, keySize));
    }

    private int rcolumnLevel(SkipListSetPage page, long setIndex, int level) {
        int keyLengthSize = page.map.keyLengthSize;
        int keySize = page.map.keySize;
        int entrySize = page.map.entrySize;
        long offset = map.startOfPayload(setIndex, entrySize, keyLengthSize, keySize) + 1 + (level * cColumKeySize);
        return MapStore.DEFAULT.readInt(page.map.array, offset);
    }

    private void wcolumnLevel(SkipListSetPage page, long setIndex, int level, long v) {
        int keyLengthSize = page.map.keyLengthSize;
        int keySize = page.map.keySize;
        int entrySize = page.map.entrySize;
        long offset = map.startOfPayload(setIndex, entrySize, keyLengthSize, keySize) + 1 + (level * cColumKeySize);
        MapStore.DEFAULT.writeInt(page.map.array, offset, (int) v);
    }

    private int startOfPayload(int maxHeight) {
        return 1 + (cColumKeySize * maxHeight);
    }

    private byte[] getColumnPayload(SkipListSetPage page, long setIndex, int maxHeight) {
        int keyLengthSize = page.map.keyLengthSize;
        int keySize = page.map.keySize;
        int entrySize = page.map.entrySize;
        long startOfPayload = map.startOfPayload(setIndex, entrySize, keyLengthSize, keySize);
        int size = (page.map.payloadLengthSize + page.map.payloadSize) - startOfPayload(maxHeight);
        byte[] payload = new byte[size];
        MapStore.DEFAULT.read(page.map.array, (int) (startOfPayload + 1 + (maxHeight * cColumKeySize)), payload, 0, size);
        return payload;
    }

    // debugging aids
    /**
     *
     * @param page
     * @param keyToString
     */
    public void sltoSysOut(SkipListSetPage page, BytesToString keyToString) {
        if (keyToString == null) {
            keyToString = new BytesToBytesString();
        }
        long atIndex = page.headIndex;
        while (atIndex != -1) {
            toSysOut(page, atIndex, keyToString);
            atIndex = rcolumnLevel(page, atIndex, 1);
        }
    }

    /**
     *
     */
    static abstract public class BytesToString {

        /**
         *
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
         *
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
         *
         * @param bytes
         * @return
         */
        @Override
        public String bytesToString(byte[] bytes) {
            return new String(bytes); // UString.toString(bytes, ",");
        }
    }

    private void toSysOut(SkipListSetPage page, long index, BytesToString keyToString) {
        byte[] key = map.getKeyAtIndex(page.map, index);
        System.out.print("\t" + keyToString.bytesToString(key) + " - ");
        int l = columnLength(page, index);
        for (int i = 0; i < l; i++) {
            if (i != 0) {
                System.out.print(",");

            }
            int ni = rcolumnLevel(page, index, i);
            if (ni == -1) {
                System.out.print("NULL");

            } else {
                byte[] nkey = map.getKeyAtIndex(page.map, ni);
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
