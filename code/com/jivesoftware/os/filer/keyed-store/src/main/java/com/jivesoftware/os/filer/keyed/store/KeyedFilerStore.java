package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public interface KeyedFilerStore {

    <R> R execute(byte[] keyBytes, long newFilerInitialCapacity, FilerTransaction<Filer, R> transaction) throws IOException;

    <R> R executeRewrite(byte[] keyBytes, long newFilerInitialCapacity, RewriteFilerTransaction<Filer, R> transaction) throws IOException;

    Iterator<IBA> keysIterator();

    void close();
}
