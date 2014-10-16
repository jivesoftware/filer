package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.AutoResizingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
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
        return openOrCreate(getChunkStoreFile(new String[]{chunkPath}, chunkPrefix, 0), chunkStoreCapacityInBytes, autoResize);
    }

    public MultiChunkStore initializeMulti(String[] chunkPaths, String chunkPrefix, int numberOfChunkStores, long chunkStoreCapacityInBytes, boolean autoResize)
            throws Exception {

        MultiChunkStoreBuilder builder = new MultiChunkStoreBuilder();

        for (int index = 0; index < chunkPaths.length; index++) {
            File chunkStoreFile = getChunkStoreFile(chunkPaths, chunkPrefix, index);
            builder.addChunkStore(openOrCreate(chunkStoreFile, chunkStoreCapacityInBytes, autoResize));

        }
        return builder.build();
    }

    private ChunkStore openOrCreate(File chunkStoreFile, long chunkStoreCapacityInBytes, boolean autoResize) throws Exception {

        if (chunkStoreFile.exists() && chunkStoreFile.length() > 0) {
            return open(chunkStoreFile, new FileBackedMemMappedByteBufferFactory(chunkStoreFile), chunkStoreCapacityInBytes, autoResize);
        } else {
            return create(chunkStoreFile, new FileBackedMemMappedByteBufferFactory(chunkStoreFile), chunkStoreCapacityInBytes, autoResize);
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

    //FileBackedMemMappedByteBufferFactory bufferFactory = new FileBackedMemMappedByteBufferFactory(chunkStoreFile);
    private File getChunkStoreFile(String[] chunkPaths, String chunkPrefix, int index) {
        return new File(chunkPaths[index % chunkPaths.length], chunkPrefix + "-" + index + ".chunk");
    }

    public ChunkStore open(Object lock, ByteBufferFactory bufferFactory, long chunkStoreCapacityInBytes, boolean autoResize) throws Exception {

        ChunkStore chunkStore;
        if (autoResize) {
            Filer filer = new AutoResizingByteBufferBackedFiler(lock, chunkStoreCapacityInBytes, bufferFactory);
            chunkStore = new ChunkStore(new SubsetableFiler(filer, 0, Long.MAX_VALUE, 0));
        } else {
            Filer filer = new ByteBufferBackedFiler(lock, bufferFactory.allocate(chunkStoreCapacityInBytes));
            chunkStore = new ChunkStore(new SubsetableFiler(filer, 0, chunkStoreCapacityInBytes, 0));
        }
        chunkStore.open();
        return chunkStore;
    }

    public ChunkStore create(Object lock, ByteBufferFactory bufferFactory, long chunkStoreCapacityInBytes, boolean autoResize) throws Exception {

        ChunkStore chunkStore;

        chunkStore = new ChunkStore();
        chunkStore.setup(referenceNumber);
        if (autoResize) {
            Filer filer = new AutoResizingByteBufferBackedFiler(lock, chunkStoreCapacityInBytes, bufferFactory);
            chunkStore.createAndOpen(new SubsetableFiler(filer, 0, Long.MAX_VALUE, 0));
        } else {
            Filer filer = new ByteBufferBackedFiler(lock, bufferFactory.allocate(chunkStoreCapacityInBytes));
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
