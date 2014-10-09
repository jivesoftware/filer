package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.AutoResizingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.SubsetableFiler;
import java.io.File;

/**
 *
 */
public class ChunkStoreInitializer {

    private static final long referenceNumber = 1;

    public ChunkStore initialize(String chunkPath, String chunkPrefix, long chunkStoreCapacityInBytes, boolean autoResize) throws Exception {
        return initialize(getChunkStoreFile(new String[] { chunkPath }, chunkPrefix, 0), chunkStoreCapacityInBytes, autoResize);
    }

    public MultiChunkStore initializeMulti(String[] chunkPaths, String chunkPrefix, int numberOfChunkStores, long chunkStoreCapacityInBytes, boolean autoResize)
        throws Exception {

        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        for (int index = 0; index < chunkStores.length; index++) {
            chunkStores[index] = initialize(getChunkStoreFile(chunkPaths, chunkPrefix, index), chunkStoreCapacityInBytes, autoResize);
        }

        return new MultiChunkStore(chunkStores);
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

    private ChunkStore initialize(File chunkStoreFile, long chunkStoreCapacityInBytes, boolean autoResize) throws Exception {
        FileBackedMemMappedByteBufferFactory bufferFactory = new FileBackedMemMappedByteBufferFactory(chunkStoreFile);

        ChunkStore chunkStore;
        if (chunkStoreFile.exists() && chunkStoreFile.length() > 0) {
            Filer filer;
            if (autoResize) {
                filer = new AutoResizingByteBufferBackedFiler(chunkStoreFile, chunkStoreFile.length(), bufferFactory);
                chunkStore = new ChunkStore(new SubsetableFiler(filer, 0, Long.MAX_VALUE, 0));
            } else {
                filer = new ByteBufferBackedFiler(chunkStoreFile, bufferFactory.allocate(chunkStoreCapacityInBytes));
                chunkStore = new ChunkStore(new SubsetableFiler(filer, 0, chunkStoreCapacityInBytes, 0));
            }
            chunkStore.open();
        } else {
            chunkStore = new ChunkStore();
            chunkStore.setup(referenceNumber);

            Filer filer;
            if (autoResize) {
                filer = new AutoResizingByteBufferBackedFiler(chunkStoreFile, chunkStoreCapacityInBytes, bufferFactory);
                chunkStore.open(new SubsetableFiler(filer, 0, Long.MAX_VALUE, 0));
            } else {
                filer = new ByteBufferBackedFiler(chunkStoreFile, bufferFactory.allocate(chunkStoreCapacityInBytes));
                chunkStore.open(new SubsetableFiler(filer, 0, chunkStoreCapacityInBytes, 0));
            }
        }
        return chunkStore;
    }

}
