package com.jivesoftware.os.filer.io.map;

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
        int headIndex,
        byte[] headKey,
        SkipListComparator valueComparator) {
        this.mapContext = mapContext;
        this.headIndex = headIndex;
        this.headKey = headKey;
        this.keyComparator = valueComparator;
        this.maxHeight = 9; // TODO expose to constructor
    }

}
