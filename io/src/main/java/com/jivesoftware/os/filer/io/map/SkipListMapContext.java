package com.jivesoftware.os.filer.io.map;

import java.util.Arrays;

/**
 *
 * @author jonathan
 */
public class SkipListMapContext {

    public final MapContext mapContext;
    byte maxHeight;
    SkipListComparator keyComparator;
    int headIndex;
    byte[] headKey;

    public SkipListMapContext(MapContext mapContext,
        byte maxHeight,
        int headIndex,
        byte[] headKey,
        SkipListComparator valueComparator) {
        this.mapContext = mapContext;
        this.maxHeight = maxHeight;
        this.headIndex = headIndex;
        this.headKey = headKey;
        this.keyComparator = valueComparator;
    }

    @Override
    public String toString() {
        return "SkipListMapContext{"
            + "mapContext=" + mapContext
            + ", maxHeight=" + maxHeight
            + ", keyComparator=" + keyComparator
            + ", headIndex=" + headIndex
            + ", headKey=" + Arrays.toString(headKey)
            + '}';
    }

}
