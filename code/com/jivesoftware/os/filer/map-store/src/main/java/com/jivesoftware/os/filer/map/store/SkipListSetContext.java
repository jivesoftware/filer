package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;

/**
 *
 * @author jonathan
 */
public class SkipListSetContext {

    /**
     *
     */
    final MapContext mapContext;
    byte maxHeight;
    SkipListComparator valueComparator;
    int headIndex;
    byte[] headKey;

    /**
     *
     * @param mapContext
     * @param headKey
     * @param valueComparator
     */
    public SkipListSetContext(MapContext mapContext,
        int headIndex,
        byte[] headKey,
        SkipListComparator valueComparator) {
        this.mapContext = mapContext;
        this.headKey = headKey;
        this.valueComparator = valueComparator;
        this.maxHeight = heightFit(mapContext.capacity);
    }

    /**
     *
     * @param bytes
     * @return
     */
    public boolean isHeadKey(byte[] bytes) {
        if (bytes.length != headKey.length) {
            return false;
        }
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != headKey[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     *
     * @param startOfAKey
     * @param startOfBKey
     * @return
     */
    public int compare(Filer f, int startOfAKey, int startOfBKey) throws IOException {
        return valueComparator.compare(f, startOfAKey, f, startOfBKey, mapContext.keySize);
    }

    /**
     *
     * @param startOfAKey
     * @param _key
     * @return
     */
    public int compare(Filer f, int startOfAKey, byte[] _key) throws IOException {
        return valueComparator.compare(f, startOfAKey, new ByteArrayFiler(_key), 0, mapContext.keySize);
    }

    /**
     *
     * @param _length
     * @return
     */
    public static byte heightFit(long _length) {
        byte h = (byte) 1; // room for back links
        for (byte i = 1; i < 65; i++) { // 2^64 == long so why go anyfuther
            if (_length < Math.pow(2, i)) {
                return (byte) (h + i);
            }
        }
        return 64;
    }
}
