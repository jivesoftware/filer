package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
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
 *
 * @author jonathan
 */
public class SkipListSet {

    final MapStore map = MapStore.INSTANCE;

    public SkipListSet() {
    }

    private static final int cColumKeySize = 4; // stores the int index of the key it points to !! could make dynamic to save space

    public long computeFilerSize(
        int _maxCount,
        int _keySize,
        int _payloadSize) throws IOException {
        _maxCount += 2;
        byte maxHeight = SkipListSetContext.heightFit(_maxCount);
        int payloadSize = slpayloadSize(maxHeight) + _payloadSize;
        return map.computeFilerSize(_maxCount, _keySize, false, payloadSize, false);
    }

    public SkipListSetContext create(
        int _maxCount,
        byte[] headKey,
        int _keySize,
        int _payloadSize,
        SkipListComparator _valueComparator,
        Filer filer) throws IOException {
        if (headKey.length != _keySize) {
            throw new RuntimeException("Expected that headKey.length == keySize");
        }
        _maxCount += 2;
        byte maxHeight = SkipListSetContext.heightFit(_maxCount);
        int payloadSize = slpayloadSize(maxHeight) + _payloadSize;
        MapContext mapContext = map.create(_maxCount, _keySize, false, payloadSize, false, filer);
        byte[] headPayload = new byte[payloadSize];
        Arrays.fill(headPayload, Byte.MIN_VALUE);
        int headKeyIndex = (int) map.add(filer, mapContext, (byte) 1, headKey, newColumn(headPayload, maxHeight, maxHeight));
        //stoSysOut(setPage);
        SkipListSetContext slsPage = new SkipListSetContext(mapContext, headKeyIndex, headKey, _valueComparator);
        return slsPage;
    }

    public SkipListSetContext open(byte[] headKey,
        SkipListComparator _valueComparator,
        Filer filer) throws IOException {

        MapContext mapContext = map.open(filer);
        int headKeyIndex = (int) map.get(filer, mapContext, headKey);
        if (headKeyIndex == -1) {
            throw new RuntimeException("SkipListSetPage:Invalid Page!");
        }
        SkipListSetContext slsPage = new SkipListSetContext(mapContext, headKeyIndex, headKey, _valueComparator);
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
        byte maxHeight = SkipListSetContext.heightFit(_maxCount);
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
    public long slgetCount(Filer filer, SkipListSetContext page) throws IOException {
        return map.getCount(filer) - 1; // -1 because of head
    }

    /**
     *
     * @param filer
     * @param context
     * @param _key
     * @param _payload
     * @throws java.io.IOException
     */
    public void sladd(Filer filer, SkipListSetContext context, byte[] _key, byte[] _payload) throws IOException {

        int index = (int) map.get(filer, context.mapContext, _key);
        if (index != -1) { // aready exists so just update payload
            map.setPayloadAtIndex(filer, context.mapContext, index, startOfPayload(context.maxHeight), _payload, 0, _payload.length);
            return;
        }
        int sEntrySize = context.mapContext.entrySize;
        // create a new colum for a new key
        byte[] newColumn = newColumn(_payload, context.maxHeight, (byte) -1);
        int insertsIndex = (int) map.add(filer, context.mapContext, (byte) 1, _key, newColumn);

        int level = context.maxHeight - 1;
        int ilevel = columnLength(filer, context, insertsIndex);
        int atIndex = context.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(filer, context, atIndex, level);
            if (nextIndex == -1) {
                if (level < ilevel) {
                    wcolumnLevel(filer, context, atIndex, level, insertsIndex);
                    if (level == 1) {
                        wcolumnLevel(filer, context, insertsIndex, 0, atIndex);
                    }
                }
                level--;
            } else {
                int compare = context.compare(filer,
                    map.startOfKey(nextIndex, sEntrySize),
                    map.startOfKey(insertsIndex, sEntrySize));
                if (compare == 0) {
                    throw new RuntimeException("should be impossible");
                } else if (compare < 0) { // keep looking forward
                    atIndex = nextIndex;
                } else { // insert
                    if (level < ilevel) {
                        wcolumnLevel(filer, context, insertsIndex, level, nextIndex);
                        wcolumnLevel(filer, context, atIndex, level, insertsIndex);
                        if (level == 1) {
                            wcolumnLevel(filer, context, insertsIndex, 0, atIndex);
                            wcolumnLevel(filer, context, nextIndex, 0, insertsIndex);
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
    public byte[] slfindWouldInsertAfter(Filer filer, SkipListSetContext page, byte[] _key) throws IOException {

        int index = (int) map.get(filer, page.mapContext, _key);
        if (index != -1) { // aready exists so return self
            return _key;
        }
        int sEntrySize = page.mapContext.entrySize;
        // create a new colum for a new key

        int level = page.maxHeight - 1;
        int atIndex = page.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(filer, page, atIndex, level);
            if (nextIndex == -1) {
                if (level == 1) {
                    return map.getKeyAtIndex(filer, page.mapContext, atIndex);
                }
                level--;
            } else {
                int compare = page.compare(filer, map.startOfKey(nextIndex, sEntrySize), _key);
                if (compare == 0) {
                    throw new RuntimeException("should be impossible");
                } else if (compare < 0) { // keep looking forward
                    atIndex = nextIndex;
                } else { // insert
                    if (level == 1) {
                        if (atIndex == page.headIndex) {
                            return null;
                        }
                        return map.getKeyAtIndex(filer, page.mapContext, atIndex);
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
    public byte[] slgetFirst(Filer filer, SkipListSetContext page) throws IOException {
        int firstIndex = rcolumnLevel(filer, page, page.headIndex, 1);
        if (firstIndex == -1) {
            return null;
        } else {
            return map.getKeyAtIndex(filer, page.mapContext, firstIndex);
        }
    }

    /**
     *
     * @param page
     * @param _key
     */
    public void slremove(Filer filer, SkipListSetContext page, byte[] _key) throws IOException {
        if (_key == null || _key.length == 0) {
            throw new RuntimeException("null not supported");
        }
        int removeIndex = (int) map.get(filer, page.mapContext, _key);
        if (removeIndex == -1) { // doesn't exists so return
            return;
        }
        int sEntrySize = page.mapContext.entrySize;

        int level = page.maxHeight - 1;
        int atIndex = page.headIndex;
        while (level > 0) {
            int nextIndex = rcolumnLevel(filer, page, atIndex, level);
            if (nextIndex == -1) {
                level--;
            } else {
                int compare = page.compare(filer,
                    map.startOfKey(nextIndex, sEntrySize),
                    map.startOfKey(removeIndex, sEntrySize));
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
        map.remove(filer, page.mapContext, _key);
    }

    /**
     *
     * @param page
     * @param _key
     * @return
     */
    public byte[] slgetPrior(Filer filer, SkipListSetContext page, byte[] _key) throws IOException {
        if (_key == null || _key.length == 0) {
            return null;
        }
        int index = (int) map.get(filer, page.mapContext, _key);
        if (index == -1) {
            return null;
        } else {
            int pi = rcolumnLevel(filer, page, index, 0);
            if (pi == -1) {
                return null;
            } else {
                byte[] got = map.getKeyAtIndex(filer, page.mapContext, pi);
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
    public byte[] slgetNext(Filer filer, SkipListSetContext page, byte[] _key) throws IOException {
        if (_key == null || _key.length == 0) {
            return null;
        }
        int index = (int) map.get(filer, page.mapContext, _key);
        if (index == -1) {
            return null;
        } else {
            int nextIndex = rcolumnLevel(filer, page, index, 1);
            if (nextIndex == -1) {
                return null;
            } else {
                return map.getKeyAtIndex(filer, page.mapContext, nextIndex);
            }
        }
    }

    /**
     *
     * @param page
     * @param _key
     * @return
     */
    public byte[] slgetExisting(Filer filer, SkipListSetContext page, byte[] _key) throws IOException {
        if (_key == null || _key.length == 0) {
            throw new RuntimeException("null not supported");
        }
        int index = (int) map.get(filer, page.mapContext, _key);
        if (index == -1) {
            return null;
        } else {
            return getColumnPayload(filer, page, index, page.maxHeight);
        }
    }

    /**
     *
     * @param page
     * @param key
     * @return
     */
    public byte[] slgetAfterExisting(Filer filer, SkipListSetContext page, byte[] key) throws IOException {
        if (key == null || key.length == 0) {
            throw new RuntimeException("null not supported");
        }
        int index = (int) map.get(filer, page.mapContext, key);
        if (index == -1) {
            return null;
        }
        int nextIndex = rcolumnLevel(filer, page, index, 1);
        return map.getKeyAtIndex(filer, page.mapContext, nextIndex);
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
    public void slgetSlice(Filer filer, SkipListSetContext page, byte[] from, byte[] to, int _max, ExtractorStream<KeyPayload, Exception> _get) throws Exception {
        final KeyPayload sent = new KeyPayload(null, null);
        int at;
        if (from != null && from.length > 0) {
            at = (int) map.get(filer, page.mapContext, from);
            if (at == -1) {
                byte[] found = slfindWouldInsertAfter(filer, page, from);
                if (found == null) {
                    _get.stream(null); // done cause from doesn't exist
                    return;
                }
                at = (int) map.get(filer, page.mapContext, found);
                at = rcolumnLevel(filer, page, at, 1); // move to next because we found the one before
            }

        } else {
            at = page.headIndex;
        }

        done:
        while (at != -1) {
            if (to != null) {
                int compare = page.compare(filer, map.startOfKey(at, page.mapContext.entrySize), to);
                if (compare > 0) {
                    break;
                }
            }
            byte[] key = map.getKeyAtIndex(filer, page.mapContext, at);
            byte[] payload = getColumnPayload(filer, page, at, page.maxHeight);
            sent.key = key;
            sent.payload = payload;
            if (_get.stream(sent) != sent) {
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
        _get.stream(null);
    }

    static public interface ExtractorStream<R, E extends Throwable> {

        R stream(R v) throws E;

    }

    /**
     * this is a the lazy impl... this can be highly optimized when we have time!
     *
     * @param from
     * @param to
     * @throws Exception
     */
    public void slcopyTo(Filer f, SkipListSetContext from, final Filer t, final SkipListSetContext to) throws Exception {
        slgetSlice(f, from, null, null, -1, new ExtractorStream<KeyPayload, Exception>() {

            @Override
            public KeyPayload stream(KeyPayload v) throws Exception {
                if (v == null) {
                    return v;
                }
                sladd(t, to, v.key, v.payload);
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

    private byte columnLength(Filer f, SkipListSetContext page, int setIndex) throws IOException {
        int entrySize = page.mapContext.entrySize;
        int keyLength = page.mapContext.keyLengthSize;
        int keySize = page.mapContext.keySize;
        return map.read(f, map.startOfPayload(setIndex, entrySize, keyLength, keySize));
    }

    private int rcolumnLevel(Filer f, SkipListSetContext context, int setIndex, int level) throws IOException {
        int entrySize = context.mapContext.entrySize;
        int keyLength = context.mapContext.keyLengthSize;
        int keySize = context.mapContext.keySize;
        int offset = (int) map.startOfPayload(setIndex, entrySize, keyLength, keySize) + 1 + (level * cColumKeySize);
        return map.readInt(f, offset);
    }

    private void wcolumnLevel(Filer f, SkipListSetContext page, int setIndex, int level, int v) throws IOException {
        int entrySize = page.mapContext.entrySize;
        int keyLength = page.mapContext.keyLengthSize;
        int keySize = page.mapContext.keySize;
        int offset = (int) map.startOfPayload(setIndex, entrySize, keyLength, keySize) + 1 + (level * cColumKeySize);
        map.writeInt(f, offset, v);
    }

    private int startOfPayload(int maxHeight) {
        return 1 + (cColumKeySize * maxHeight);
    }

    private byte[] getColumnPayload(Filer f, SkipListSetContext page, int setIndex, int maxHeight) throws IOException {
        int entrySize = page.mapContext.entrySize;
        int keyLength = page.mapContext.keyLengthSize;
        int keySize = page.mapContext.keySize;
        int startOfPayload = (int) map.startOfPayload(setIndex, entrySize, keyLength, keySize);
        int size = page.mapContext.payloadSize - startOfPayload(maxHeight);
        byte[] payload = new byte[size];
        map.read(f, startOfPayload + 1 + (maxHeight * cColumKeySize), payload, 0, size);
        return payload;
    }

    // debugging aids
    /**
     *
     * @param page
     * @param keyToString
     */
    public void sltoSysOut(Filer f, SkipListSetContext page, BytesToString keyToString) throws IOException {
        if (keyToString == null) {
            keyToString = new BytesToBytesString();
        }
        int atIndex = page.headIndex;
        while (atIndex != -1) {
            toSysOut(f, page, atIndex, keyToString);
            atIndex = rcolumnLevel(f, page, atIndex, 1);
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

    private void toSysOut(Filer filer, SkipListSetContext page, int index, BytesToString keyToString) throws IOException {
        byte[] key = map.getKeyAtIndex(filer, page.mapContext, index);
        System.out.print("\ti:" + index);
        System.out.print("\tv:" + keyToString.bytesToString(key) + " - \t");
        int l = columnLength(filer, page, index);
        for (int i = 0; i < l; i++) {
            if (i != 0) {
                System.out.print("),\t"+i+":(");
            } else {
                System.out.print(i+":(");
            }
            int ni = rcolumnLevel(filer, page, index, i);
            if (ni == -1) {
                System.out.print("NULL");

            } else {
                byte[] nkey = map.getKeyAtIndex(filer, page.mapContext, ni);
                if (nkey == null) {
                    System.out.println("??");
                } else {
                    System.out.print(keyToString.bytesToString(nkey));
                }

            }
        }
        System.out.println(")");
    }
}
