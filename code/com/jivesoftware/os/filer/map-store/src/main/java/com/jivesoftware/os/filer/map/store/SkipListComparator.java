package com.jivesoftware.os.filer.map.store;

import java.io.IOException;

/**
 *
 * @author jonathan
 */
public interface SkipListComparator {

    public int compare(byte[] a, byte[] b) throws IOException;

}
