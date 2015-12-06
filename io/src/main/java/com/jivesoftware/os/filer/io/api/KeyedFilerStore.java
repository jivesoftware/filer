package com.jivesoftware.os.filer.io.api;

import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public interface KeyedFilerStore<H, M> {

    <R> R read(byte[] keyBytes, H newFilerInitialCapacity, final ChunkTransaction<M, R> transaction, StackBuffer stackBuffer) throws IOException,
        InterruptedException;

    <R> void readEach(byte[][] eachKeyBytes,
        H newFilerInitialCapacity,
        IndexAlignedChunkTransaction<M, R> chunkTransaction,
        R[] results,
        StackBuffer stackBuffer) throws IOException,
        InterruptedException;

    <R> R readWriteAutoGrow(byte[] keyBytes,
        H newFilerInitialCapacity,
        ChunkTransaction<M, R> transaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException;

    <R> R writeNewReplace(byte[] keyBytes,
        H newFilerInitialCapacity,
        ChunkTransaction<M, R> transaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException;

    boolean stream(List<KeyRange> ranges, KeyValueStore.EntryStream<byte[], Filer> stream, StackBuffer stackBuffer) throws IOException, InterruptedException;

    /**
     * @param ranges nullable null means all keys.
     * @param stream
     * @return
     * @throws IOException
     */
    boolean streamKeys(List<KeyRange> ranges, KeyValueStore.KeyStream<byte[]> stream, StackBuffer stackBuffer) throws IOException, InterruptedException;

    long size(StackBuffer stackBuffer) throws IOException, InterruptedException;

    void close();
}
