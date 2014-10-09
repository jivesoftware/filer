package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.map.store.extractors.ExtractIndex;
import com.jivesoftware.os.filer.map.store.extractors.ExtractKey;
import com.jivesoftware.os.filer.map.store.extractors.ExtractPayload;
import com.jivesoftware.os.filer.map.store.extractors.Extractor;
import com.jivesoftware.os.filer.map.store.extractors.ExtractorStream;
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

    private final Extractor<Integer> extractIndex = new ExtractIndex();
    final MapStore map = new MapStore(extractIndex, new ExtractKey(), new ExtractPayload());

    public SkipListSet() {
    }

    private static final int cColumKeySize = 4; // stores the int index of the key it points to !! could make dynamic to save space

    /**
     * new
     *
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
    public SkipListSetPage slallocate(byte[] id,
            long version, int _maxCount, byte[] headKey, int _keySize, int _payloadSize, SkipListComparator _valueComparator, ByteBufferFactory factory) {
        if (headKey.length != _keySize) {
            throw new RuntimeException("Expected that headKey.length == keySize");
        }
        _maxCount += 2;
        byte maxHeight = SkipListSetPage.heightFit(_maxCount);
        int payloadSize = slpayloadSize(maxHeight) + _payloadSize;

        MapChunk setPage = map.allocate((byte) 0, (byte) 0, id, version, _maxCount, _keySize, payloadSize, factory);
        byte[] headPayload = new byte[payloadSize];
        Arrays.fill(headPayload, Byte.MIN_VALUE);
        map.add(setPage, (byte) 1, headKey, newColumn(headPayload, maxHeight, maxHeight));
        //stoSysOut(setPage);
        SkipListSetPage slsPage = new SkipListSetPage(setPage, headKey, _valueComparator);
        slsPage.init();
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

        int index = map.get(page.map, _key, extractIndex);
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
        int atIndex = page.headIndex;
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
                int compare = page.compare(map.startOfKey(nextIndex, skeySize, spayloadSize), map.startOfKey(insertsIndex, skeySize, spayloadSize));
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

        int index = map.get(page.map, _key, extractIndex);
        if (index != -1) { // aready exists so return self
            return _key;
        }
        int skeySize = page.map.keySize;
        int spayloadSize = page.map.payloadSize;
        // create a new colum for a new key

        int level = page.maxHeight - 1;
        int atIndex = page.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(page, atIndex, level);
            if (nextIndex == -1) {
                if (level == 1) {
                    return map.getKeyAtIndex(page.map, atIndex);
                }
                level--;
            } else {
                int compare = page.compare(map.startOfKey(nextIndex, skeySize, spayloadSize), _key);
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
        int removeIndex = map.get(page.map, _key, extractIndex);
        if (removeIndex == -1) { // doesn't exists so return
            return;
        }
        int skeySize = page.map.keySize;
        int spayloadSize = page.map.payloadSize;

        int level = page.maxHeight - 1;
        int atIndex = page.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(page, atIndex, level);
            if (nextIndex == -1) {
                level--;
            } else {
                int compare = page.compare(map.startOfKey(nextIndex, skeySize, spayloadSize), map.startOfKey(removeIndex, skeySize, spayloadSize));
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
        int index = map.get(page.map, _key, extractIndex);
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
        int index = map.get(page.map, _key, extractIndex);
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
        int index = map.get(page.map, _key, extractIndex);
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
        int index = map.get(page.map, key, extractIndex);
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
    public void slgetSlice(SkipListSetPage page, byte[] from, byte[] to, int _max, ExtractorStream<KeyPayload, Exception> _get) throws Exception {
        final KeyPayload sent = new KeyPayload(null, null);
        int at;
        if (from != null && from.length > 0) {
            at = map.get(page.map, from, extractIndex);
            if (at == -1) {
                byte[] found = slfindWouldInsertAfter(page, from);
                if (found == null) {
                    _get.stream(null); // done cause from doesn't exist
                    return;
                }
                at = map.get(page.map, found, extractIndex);
                at = rcolumnLevel(page, at, 1); // move to next because we found the one before
            }

        } else {
            at = page.headIndex;
        }

        done:
        while (at != -1) {
            if (to != null) {
                int compare = page.compare(map.startOfKey(at, page.map.keySize, page.map.payloadSize), to);
                if (compare > 0) {
                    break;
                }
            }
            byte[] key = map.getKeyAtIndex(page.map, at);
            byte[] payload = getColumnPayload(page, at, page.maxHeight);
            sent.key = key;
            sent.payload = payload;
            if (_get.stream(sent) != sent) {
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
        _get.stream(null);
    }

    /**
     * this is a the lazy impl... this can be highly optimized when we have time!
     *
     * @param from
     * @param to
     * @throws Exception
     */
    public void slcopyTo(SkipListSetPage from, final SkipListSetPage to) throws Exception {
        slgetSlice(from, null, null, -1, new ExtractorStream<KeyPayload, Exception>() {

            @Override
            public KeyPayload stream(KeyPayload v) throws Exception {
                if (v == null) {
                    return v;
                }
                sladd(to, v.key, v.payload);
                return v;
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

    private byte columnLength(SkipListSetPage page, int setIndex) {
        int keySize = page.map.keySize;
        int payloadSize = page.map.payloadSize;
        return page.map.read(map.startOfPayload(setIndex, keySize, payloadSize));
    }

    private int rcolumnLevel(SkipListSetPage page, int setIndex, int level) {
        int keySize = page.map.keySize;
        int payloadSize = page.map.payloadSize;
        int offset = map.startOfPayload(setIndex, keySize, payloadSize) + 1 + (level * cColumKeySize);
        return page.map.readInt(offset);
    }

    private void wcolumnLevel(SkipListSetPage page, int setIndex, int level, int v) {
        int keySize = page.map.keySize;
        int payloadSize = page.map.payloadSize;
        int offset = map.startOfPayload(setIndex, keySize, payloadSize) + 1 + (level * cColumKeySize);
        page.map.writeInt(offset, v);
    }

    private int startOfPayload(int maxHeight) {
        return 1 + (cColumKeySize * maxHeight);
    }

    private byte[] getColumnPayload(SkipListSetPage page, int setIndex, int maxHeight) {
        int keySize = page.map.keySize;
        int payloadSize = page.map.payloadSize;
        int startOfPayload = map.startOfPayload(setIndex, keySize, payloadSize);
        int size = payloadSize - startOfPayload(maxHeight);
        byte[] payload = new byte[size];
        page.map.read(startOfPayload + 1 + (maxHeight * cColumKeySize), payload, 0, size);
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
        int atIndex = page.headIndex;
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

    private void toSysOut(SkipListSetPage page, int index, BytesToString keyToString) {
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
