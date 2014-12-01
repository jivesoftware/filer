package com.jivesoftware.os.filer.keyed.store;


import com.jivesoftware.os.filer.io.IBA;
import java.util.Iterator;

/**
 *
 */
public interface KeyedFilerStore {

    SwappableFiler get(byte[] keyBytes, long newFilerInitialCapacity) throws Exception;

    long sizeInBytes() throws Exception;

    Iterator<IBA> keysIterator();

    void close();
}
