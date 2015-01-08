package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import java.io.File;
import java.io.IOException;

/**
 *
 */
public class ChunkStoreInitializer {

    private static final long referenceNumber = 1;

    public ChunkStore openOrCreate(File[] dirs, String chunkName, int initialSize) throws Exception {
        FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(chunkName, dirs);
        AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(factory, initialSize, Integer.MAX_VALUE);
        if (filer.exists()) {
            return open(filer);
        } else {
            return create(filer);
        }
    }

    public boolean checkExists(File[] dirs, String chunkName) throws IOException {
        FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(chunkName, dirs);
        return new AutoGrowingByteBufferBackedFiler(factory, 1024, Integer.MAX_VALUE).exists();
    }

    public ChunkStore open(AutoGrowingByteBufferBackedFiler filer) throws Exception {
        ChunkStore chunkStore = new ChunkStore(filer);
        chunkStore.open();
        return chunkStore;
    }

    public ChunkStore create(AutoGrowingByteBufferBackedFiler filer)
        throws Exception {

        ChunkStore chunkStore = new ChunkStore();
        chunkStore.setup(referenceNumber);
        chunkStore.createAndOpen(filer);
        return chunkStore;
    }

}
