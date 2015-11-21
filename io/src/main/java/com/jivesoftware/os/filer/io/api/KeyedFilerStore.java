package com.jivesoftware.os.filer.io.api;

import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public interface KeyedFilerStore<H, M> {

    <R> R read(byte[] keyBytes, H newFilerInitialCapacity, final ChunkTransaction<M, R> transaction, StackBuffer stackBuffer) throws IOException;

    <R> List<R> readEach(byte[][] eachKeyBytes, H newFilerInitialCapacity, ChunkTransaction<M, R> chunkTransaction, StackBuffer stackBuffer) throws IOException;

    <R> R readWriteAutoGrow(byte[] keyBytes, H newFilerInitialCapacity, final ChunkTransaction<M, R> transaction, StackBuffer stackBuffer) throws IOException;

    <R> R writeNewReplace(byte[] keyBytes, H newFilerInitialCapacity, final ChunkTransaction<M, R> transaction, StackBuffer stackBuffer) throws IOException;

    boolean stream(List<KeyRange> ranges, KeyValueStore.EntryStream<byte[], Filer> stream, StackBuffer stackBuffer) throws IOException;

    /**
     *
     * @param ranges nullable null means all keys.
     * @param stream
     * @return
     * @throws IOException
     */
    boolean streamKeys(List<KeyRange> ranges, KeyValueStore.KeyStream<byte[]> stream, StackBuffer stackBuffer) throws IOException;

    void close();
}
