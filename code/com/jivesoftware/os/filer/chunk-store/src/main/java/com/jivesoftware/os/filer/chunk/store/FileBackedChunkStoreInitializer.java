package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.RandomAccessFiler;
import java.io.File;

/**
 *
 */
public class FileBackedChunkStoreInitializer {

    private static final long referenceNumber = 1;

    public ChunkStore initialize(String chunkFile, boolean sync) throws Exception {
        File chunkStoreFile = new File(chunkFile);

        ChunkStore chunkStore;
        if (chunkStoreFile.exists() && chunkStoreFile.length() > 0) {
            ConcurrentFiler filer = new RandomAccessFiler(chunkStoreFile, "rw" + ((sync) ? "s" : ""));
            chunkStore = new ChunkStore(filer);
            chunkStore.open();
        } else {
            chunkStore = new ChunkStore();
            chunkStore.setup(referenceNumber);

            ConcurrentFiler filer = new RandomAccessFiler(chunkStoreFile, "rw" + ((sync) ? "s" : ""));
            chunkStore.createAndOpen(filer);
        }
        return chunkStore;
    }
}
