package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.io.IOException;

/**
 *
 */
public interface KeyedFilerStore {

    <R> R execute(byte[] keyBytes, long newFilerInitialCapacity, FilerTransaction<Filer, R> transaction) throws IOException;

    <R> R executeRewrite(byte[] keyBytes, long newFilerInitialCapacity, RewriteFilerTransaction<Filer, R> transaction) throws IOException;

    void stream(KeyValueStore.EntryStream<IBA, Filer> stream) throws IOException;

    void streamKeys(KeyValueStore.KeyStream<IBA> stream) throws IOException;

    void close();
}
