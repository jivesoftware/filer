package com.jivesoftware.os.filer.map.store;

/**
 *
 * @author jonathan
 */
public interface SkipListComparator {

    /**
     * Same behavior as javas Comparator.
     *
     * @param a
     * @param astart
     * @param b
     * @param bstart
     * @param length
     * @return
     */
    public int compare(MapChunk a, int astart, MapChunk b, int bstart, int length);

    /**
     * Should return the maximum number of items between a and b.
     * if a and b where ints the thre result would simple be Math.max(a,b)-Math.min(a,b).
     * if a and b where lowercase string 'aa' and 'az'. Then the result is 26.
     *
     *
     * @param a
     * @param b
     * @return
     */
    public long range(byte[] a, byte[] b);
}