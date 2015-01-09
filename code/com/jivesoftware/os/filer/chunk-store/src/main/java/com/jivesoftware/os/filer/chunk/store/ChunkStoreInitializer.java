package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import java.io.File;
import java.io.IOException;

/**
 *
 */
public class ChunkStoreInitializer {

    private static final long referenceNumber = 1;

    public ChunkStore openOrCreate(File[] dirs, String chunkName, long initialSize) throws Exception {
        FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(chunkName, dirs);
        AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(factory, initialSize, FilerIO.chunkLength(32));
        if (filer.exists()) {
            return open(filer);
        } else {
            return create(filer);
        }
    }

    public boolean checkExists(File[] dirs, String chunkName) throws IOException {
        FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(chunkName, dirs);
        return new AutoGrowingByteBufferBackedFiler(factory, 1024, FilerIO.chunkLength(32)).exists();
    }

    public ChunkStore open(ByteBufferFactory filer, long segmentSize) throws Exception {
        return open(new AutoGrowingByteBufferBackedFiler(filer, segmentSize, segmentSize));
    }

    private ChunkStore open(AutoGrowingByteBufferBackedFiler filer) throws Exception {
        ChunkStore chunkStore = new ChunkStore(filer);
        chunkStore.open();
        return chunkStore;
    }

    public ChunkStore create(ByteBufferFactory factory, long segmentSize) throws Exception {
        return create(new AutoGrowingByteBufferBackedFiler(factory, segmentSize, segmentSize));
    }

    private ChunkStore create(AutoGrowingByteBufferBackedFiler filer) throws Exception {
        ChunkStore chunkStore = new ChunkStore();
        chunkStore.setup(referenceNumber);
        chunkStore.createAndOpen(filer);
        return chunkStore;
    }

}
