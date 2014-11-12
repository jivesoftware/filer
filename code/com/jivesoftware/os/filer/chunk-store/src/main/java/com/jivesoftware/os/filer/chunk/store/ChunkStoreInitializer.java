package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.AutoResizingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import java.io.File;
import java.util.ArrayList;
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
        File chunkStoreFile = getChunkStoreFile(new String[] { chunkPath }, chunkPrefix, 0);
        ByteBufferProvider byteBufferProvider = new ByteBufferProvider(chunkStoreFile.getName(),
            new FileBackedMemMappedByteBufferFactory(chunkStoreFile.getParentFile()));
        return openOrCreate(chunkStoreFile, byteBufferProvider, chunkStoreCapacityInBytes, autoResize, concurrencyLevel);
    }

    public MultiChunkStore initializeMultiFileBacked(String[] chunkPaths,
        String chunkPrefix,
        int numberOfChunkStores,
        long chunkStoreCapacityInBytes,
        boolean autoResize,
        int concurrencyLevel)
        throws Exception {

        MultiChunkStoreBuilder builder = new MultiChunkStoreBuilder();

        for (int index = 0; index < numberOfChunkStores; index++) {
            File chunkStoreFile = getChunkStoreFile(chunkPaths, chunkPrefix, index);
            ByteBufferProvider byteBufferProvider = new ByteBufferProvider(chunkStoreFile.getName(),
                new FileBackedMemMappedByteBufferFactory(chunkStoreFile.getParentFile()));
            builder.addChunkStore(openOrCreate(chunkStoreFile, byteBufferProvider, chunkStoreCapacityInBytes, autoResize, concurrencyLevel));
        }
        return builder.build();
    }

    public MultiChunkStore copyToMultiFileBacked(MultiChunkStore from,
        String[] chunkPaths,
        String chunkPrefix,
        boolean autoResize,
        int concurrencyLevel)
        throws Exception {

        MultiChunkStoreBuilder builder = new MultiChunkStoreBuilder();

        for (int index = 0; index < from.chunkStores.length; index++) {
            File chunkStoreFile = getChunkStoreFile(chunkPaths, chunkPrefix, index);
            ByteBufferProvider byteBufferProvider = new ByteBufferProvider(chunkStoreFile.getName(),
                new FileBackedMemMappedByteBufferFactory(chunkStoreFile.getParentFile()));
            ChunkStore chunkStore = openOrCreate(chunkStoreFile, byteBufferProvider, from.chunkStores[index].sizeInBytes(), autoResize, concurrencyLevel);
            builder.addChunkStore(chunkStore);
            from.chunkStores[index].copyTo(chunkStore);
        }
        return builder.build();
    }

    public MultiChunkStore initializeMultiByteBufferBacked(
        String keyPrefix,
        ByteBufferFactory byteBufferFactory,
        int numberOfChunkStores,
        long chunkStoreCapacityInBytes,
        boolean autoResize,
        int concurrencyLevel)
        throws Exception {

        MultiChunkStoreBuilder builder = new MultiChunkStoreBuilder();

        for (int index = 0; index < numberOfChunkStores; index++) {
            builder.addChunkStore(create(new Object(), new ByteBufferProvider(keyPrefix + "-" + index, byteBufferFactory),
                chunkStoreCapacityInBytes, autoResize, concurrencyLevel));
        }
        return builder.build();
    }

    private ChunkStore openOrCreate(File chunkStoreFile,
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

    private File getChunkStoreFile(String[] chunkPaths, String chunkPrefix, int index) {
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
            ConcurrentFiler filer = new AutoResizingByteBufferBackedFiler(lock, chunkStoreCapacityInBytes, byteBufferProvider,
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
            ConcurrentFiler filer = new AutoResizingByteBufferBackedFiler(lock, chunkStoreCapacityInBytes, byteBufferProvider,
                new Semaphore(concurrencyLevel), concurrencyLevel);
            chunkStore.createAndOpen(filer);
        } else {
            ConcurrentFiler filer = new ByteBufferBackedFiler(lock, byteBufferProvider.allocate(chunkStoreCapacityInBytes));
            chunkStore.createAndOpen(filer);
        }
        return chunkStore;
    }

    static public class MultiChunkStoreBuilder {

        private final ArrayList<ChunkStore> stores = new ArrayList<>();

        public MultiChunkStoreBuilder addChunkStore(ChunkStore chunkStore) {
            stores.add(chunkStore);
            return this;
        }

        public MultiChunkStore build() {
            return new MultiChunkStore(stores.toArray(new ChunkStore[stores.size()]));
        }
    }

}
