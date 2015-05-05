package com.jivesoftware.os.filer.io.map;

import java.io.IOException;

/**
 *
 * @author jonathan
 */
public interface SkipListComparator {

    public int compare(byte[] a, byte[] b) throws IOException;

}
