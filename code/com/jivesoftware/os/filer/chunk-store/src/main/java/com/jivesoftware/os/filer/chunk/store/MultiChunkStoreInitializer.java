package com.jivesoftware.os.filer.chunk.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStoreConcurrentFilerFactory.Builder;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import java.io.File;

/**
 *
 */
public class MultiChunkStoreInitializer {

    private final ChunkStoreInitializer chunkStoreInitializer;

    public MultiChunkStoreInitializer(ChunkStoreInitializer chunkStoreInitializer) {
        this.chunkStoreInitializer = chunkStoreInitializer;
    }


    public MultiChunkStoreConcurrentFilerFactory initializeMultiFileBacked(
        String[] chunkPaths,
        String chunkPrefix,
        int numberOfChunkStores,
        long chunkStoreCapacityInBytes,
        boolean autoResize,
        int concurrencyLevel,
        int stripingLevel)
        throws Exception {

        Builder builder = new MultiChunkStoreConcurrentFilerFactory.Builder();
        builder.setStripingLevel(stripingLevel);

        for (int index = 0; index < numberOfChunkStores; index++) {
            File chunkStoreFile = chunkStoreInitializer.getChunkStoreFile(chunkPaths, chunkPrefix, index);
            ByteBufferProvider byteBufferProvider = new ByteBufferProvider(chunkStoreFile.getName().getBytes(Charsets.UTF_8),
                new FileBackedMemMappedByteBufferFactory(chunkStoreFile.getParentFile()));
            ChunkStore chunkStore = chunkStoreInitializer.openOrCreate(chunkStoreFile, byteBufferProvider, chunkStoreCapacityInBytes, autoResize,
                concurrencyLevel);
            builder.addChunkStore(chunkStore);
        }
        return builder.build();
    }

    
    public MultiChunkStoreConcurrentFilerFactory initializeMultiByteBufferBacked(
        String keyPrefix,
        ByteBufferFactory byteBufferFactory,
        int numberOfChunkStores,
        long chunkStoreCapacityInBytes,
        boolean autoResize,
        int concurrencyLevel,
        int stripingLevel)
        throws Exception {

        Builder builder = new MultiChunkStoreConcurrentFilerFactory.Builder();
        builder.setStripingLevel(stripingLevel);

        for (int index = 0; index < numberOfChunkStores; index++) {

            builder.addChunkStore(chunkStoreInitializer.create(new Object(),
                new ByteBufferProvider((keyPrefix + "-" + index).getBytes(Charsets.UTF_8), byteBufferFactory),
                chunkStoreCapacityInBytes, autoResize, concurrencyLevel));
        }
        return builder.build();
    }

    public MultiChunkStoreConcurrentFilerFactory copyToMultiFileBacked(MultiChunkStoreConcurrentFilerFactory from,
        String[] chunkPaths,
        String chunkPrefix,
        boolean autoResize,
        int concurrencyLevel,
        int stripingLevel)
        throws Exception {

        Builder builder = new MultiChunkStoreConcurrentFilerFactory.Builder();
        builder.setStripingLevel(stripingLevel);

        for (int index = 0; index < from.chunkStores.length; index++) {
            File chunkStoreFile = chunkStoreInitializer.getChunkStoreFile(chunkPaths, chunkPrefix, index);
            ByteBufferProvider byteBufferProvider = new ByteBufferProvider(chunkStoreFile.getName().getBytes(Charsets.UTF_8),
                new FileBackedMemMappedByteBufferFactory(chunkStoreFile.getParentFile()));
            ChunkStore chunkStore = chunkStoreInitializer.openOrCreate(chunkStoreFile, byteBufferProvider, from.chunkStores[index].sizeInBytes(), autoResize,
                concurrencyLevel);
            builder.addChunkStore(chunkStore);
            from.chunkStores[index].copyTo(chunkStore);
        }
        return builder.build();
    }

}
