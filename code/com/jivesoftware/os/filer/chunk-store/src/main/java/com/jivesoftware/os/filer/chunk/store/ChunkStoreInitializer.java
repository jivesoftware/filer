package com.jivesoftware.os.filer.chunk.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.ResizableByteBuffer;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import java.io.File;
import java.util.concurrent.Semaphore;

/**
 *
 */
public class ChunkStoreInitializer {

    private static final long referenceNumber = 1;

    public ChunkStore initialize(String chunkPath,
        String chunkPrefix,
        long chunkStoreCapacityInBytes,
        boolean autoResize,
        int concurrencyLevel)
        throws Exception {
        File chunkStoreFile = getChunkStoreFile(new String[]{chunkPath}, chunkPrefix, 0);
        ByteBufferProvider byteBufferProvider = new ByteBufferProvider(chunkStoreFile.getName().getBytes(Charsets.UTF_8),
            new FileBackedMemMappedByteBufferFactory(chunkStoreFile.getParentFile()));
        return openOrCreate(chunkStoreFile, byteBufferProvider, chunkStoreCapacityInBytes, autoResize, concurrencyLevel);
    }

    ChunkStore openOrCreate(File chunkStoreFile,
        ByteBufferProvider byteBufferProvider,
        long chunkStoreCapacityInBytes,
        boolean autoResize,
        int concurrencyLevel)
        throws Exception {
        if (chunkStoreFile.exists() && chunkStoreFile.length() > 0) {
            return open(chunkStoreFile, byteBufferProvider, chunkStoreCapacityInBytes, autoResize, concurrencyLevel);
        } else {
            return create(chunkStoreFile, byteBufferProvider, chunkStoreCapacityInBytes, autoResize, concurrencyLevel);
        }
    }

    public boolean checkExists(String[] chunkPaths, String chunkPrefix, int numberOfChunkStores) {
        for (int index = 0; index < numberOfChunkStores; index++) {
            File chunkStoreFile = getChunkStoreFile(chunkPaths, chunkPrefix, index);
            if (!chunkStoreFile.exists() || !chunkStoreFile.isFile()) {
                return false;
            }
        }
        return true;
    }

    File getChunkStoreFile(String[] chunkPaths, String chunkPrefix, int index) {
        return new File(chunkPaths[index % chunkPaths.length], chunkPrefix + "-" + index + ".chunk");
    }

    public ChunkStore open(Object lock,
        ByteBufferProvider byteBufferProvider,
        long chunkStoreCapacityInBytes,
        boolean autoResize,
        int concurrencyLevel)
        throws Exception {

        ChunkStore chunkStore;
        if (autoResize) {
            ConcurrentFiler filer = new ResizableByteBuffer(lock, chunkStoreCapacityInBytes, byteBufferProvider,
                new Semaphore(concurrencyLevel), concurrencyLevel);
            chunkStore = new ChunkStore(filer);
        } else {
            ConcurrentFiler filer = new ByteBufferBackedFiler(lock, byteBufferProvider.allocate(chunkStoreCapacityInBytes));
            chunkStore = new ChunkStore(filer);
        }
        chunkStore.open();
        return chunkStore;
    }

    public ChunkStore create(Object lock,
        ByteBufferProvider byteBufferProvider,
        long chunkStoreCapacityInBytes,
        boolean autoResize,
        int concurrencyLevel)
        throws Exception {

        ChunkStore chunkStore;

        chunkStore = new ChunkStore();
        chunkStore.setup(referenceNumber);
        if (autoResize) {
            ConcurrentFiler filer = new ResizableByteBuffer(lock, chunkStoreCapacityInBytes, byteBufferProvider,
                new Semaphore(concurrencyLevel, true), concurrencyLevel);
            chunkStore.createAndOpen(filer);
        } else {
            ConcurrentFiler filer = new ByteBufferBackedFiler(lock, byteBufferProvider.allocate(chunkStoreCapacityInBytes));
            chunkStore.createAndOpen(filer);
        }
        return chunkStore;
    }

}
