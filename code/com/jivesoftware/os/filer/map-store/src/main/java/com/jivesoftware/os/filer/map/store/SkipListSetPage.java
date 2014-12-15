package com.jivesoftware.os.filer.map.store;

/**
 * @author jonathan
 */
public class SkipListSetPage {

    final MapContext chunk;
    final byte maxHeight;
    final SkipListComparator valueComparator;
    final long headIndex;
    final byte[] headKey;

    public SkipListSetPage(MapContext chunk, byte maxHeight, SkipListComparator valueComparator, long headIndex, byte[] headKey) {
        this.chunk = chunk;
        this.maxHeight = maxHeight;
        this.valueComparator = valueComparator;
        this.headIndex = headIndex;
        this.headKey = headKey;
    }
}
