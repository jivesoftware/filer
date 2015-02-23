package com.jivesoftware.os.filer.io.api;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.IBA;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public interface KeyedFilerStore<H, M> {

    <R> R read(byte[] keyBytes, H newFilerInitialCapacity, final ChunkTransaction<M, R> transaction) throws IOException;

    <R> R readWriteAutoGrow(byte[] keyBytes, H newFilerInitialCapacity, final ChunkTransaction<M, R> transaction) throws IOException;

    <R> R writeNewReplace(byte[] keyBytes, H newFilerInitialCapacity, final ChunkTransaction<M, R> transaction) throws IOException;

    boolean stream(List<KeyRange> ranges, KeyValueStore.EntryStream<IBA, Filer> stream) throws IOException;

    /**
     *
     * @param ranges nullable null means all keys.
     * @param stream
     * @return
     * @throws IOException
     */
    boolean streamKeys(List<KeyRange> ranges, KeyValueStore.KeyStream<IBA> stream) throws IOException;

    void close();

}
