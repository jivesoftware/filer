package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.AutoResizingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.SubsetableFiler;
import java.io.File;
import java.util.ArrayList;

/**
 *
 */
public class ChunkStoreInitializer {

    private static final long referenceNumber = 1;

    public ChunkStore initialize(String chunkPath, String chunkPrefix, long chunkStoreCapacityInBytes, boolean autoResize) throws Exception {
        File chunkStoreFile = getChunkStoreFile(new String[] { chunkPath }, chunkPrefix, 0);
        ByteBufferProvider byteBufferProvider = new ByteBufferProvider(chunkStoreFile.getName(),
            new FileBackedMemMappedByteBufferFactory(chunkStoreFile.getParentFile()));
        return openOrCreate(chunkStoreFile, byteBufferProvider, chunkStoreCapacityInBytes, autoResize);
    }

    public MultiChunkStore initializeMultiFileBacked(String[] chunkPaths,
        String chunkPrefix,
        int numberOfChunkStores,
        long chunkStoreCapacityInBytes,
        boolean autoResize)
        throws Exception {

        MultiChunkStoreBuilder builder = new MultiChunkStoreBuilder();

        for (int index = 0; index < numberOfChunkStores; index++) {
            File chunkStoreFile = getChunkStoreFile(chunkPaths, chunkPrefix, index);
            ByteBufferProvider byteBufferProvider = new ByteBufferProvider(chunkStoreFile.getName(),
                new FileBackedMemMappedByteBufferFactory(chunkStoreFile.getParentFile()));
            builder.addChunkStore(openOrCreate(chunkStoreFile, byteBufferProvider, chunkStoreCapacityInBytes, autoResize));
        }
        return builder.build();
    }

    public MultiChunkStore copyToMultiFileBacked(MultiChunkStore from,
        String[] chunkPaths,
        String chunkPrefix,
        boolean autoResize)
        throws Exception {

        MultiChunkStoreBuilder builder = new MultiChunkStoreBuilder();

        for (int index = 0; index < from.chunkStores.length; index++) {
            File chunkStoreFile = getChunkStoreFile(chunkPaths, chunkPrefix, index);
            ByteBufferProvider byteBufferProvider = new ByteBufferProvider(chunkStoreFile.getName(),
                new FileBackedMemMappedByteBufferFactory(chunkStoreFile.getParentFile()));
            ChunkStore chunkStore = openOrCreate(chunkStoreFile, byteBufferProvider, from.chunkStores[index].sizeInBytes(), autoResize);
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
        boolean autoResize)
        throws Exception {

        MultiChunkStoreBuilder builder = new MultiChunkStoreBuilder();

        for (int index = 0; index < numberOfChunkStores; index++) {
            builder.addChunkStore(create(new Object(), new ByteBufferProvider(keyPrefix + "-" + index, byteBufferFactory),
                chunkStoreCapacityInBytes, autoResize));
        }
        return builder.build();
    }

    private ChunkStore openOrCreate(File chunkStoreFile, ByteBufferProvider byteBufferProvider, long chunkStoreCapacityInBytes, boolean autoResize)
        throws Exception {
        if (chunkStoreFile.exists() && chunkStoreFile.length() > 0) {
            return open(chunkStoreFile, byteBufferProvider, chunkStoreCapacityInBytes, autoResize);
        } else {
            return create(chunkStoreFile, byteBufferProvider, chunkStoreCapacityInBytes, autoResize);
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

    public ChunkStore open(Object lock, ByteBufferProvider byteBufferProvider, long chunkStoreCapacityInBytes, boolean autoResize) throws Exception {

        ChunkStore chunkStore;
        if (autoResize) {
            Filer filer = new AutoResizingByteBufferBackedFiler(lock, chunkStoreCapacityInBytes, byteBufferProvider);
            chunkStore = new ChunkStore(new SubsetableFiler(filer, 0, Long.MAX_VALUE, 0));
        } else {
            Filer filer = new ByteBufferBackedFiler(lock, byteBufferProvider.allocate(chunkStoreCapacityInBytes));
            chunkStore = new ChunkStore(new SubsetableFiler(filer, 0, chunkStoreCapacityInBytes, 0));
        }
        chunkStore.open();
        return chunkStore;
    }

    public ChunkStore create(Object lock, ByteBufferProvider byteBufferProvider, long chunkStoreCapacityInBytes, boolean autoResize) throws Exception {

        ChunkStore chunkStore;

        chunkStore = new ChunkStore();
        chunkStore.setup(referenceNumber);
        if (autoResize) {
            Filer filer = new AutoResizingByteBufferBackedFiler(lock, chunkStoreCapacityInBytes, byteBufferProvider);
            chunkStore.createAndOpen(new SubsetableFiler(filer, 0, Long.MAX_VALUE, 0));
        } else {
            Filer filer = new ByteBufferBackedFiler(lock, byteBufferProvider.allocate(chunkStoreCapacityInBytes));
            chunkStore.createAndOpen(new SubsetableFiler(filer, 0, chunkStoreCapacityInBytes, 0));
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
