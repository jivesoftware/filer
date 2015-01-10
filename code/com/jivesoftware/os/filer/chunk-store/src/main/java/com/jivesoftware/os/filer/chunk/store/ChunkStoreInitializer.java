package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import java.io.File;
import java.io.IOException;

/**
 *
 */
public class ChunkStoreInitializer {

    private static final long referenceNumber = 1;

    public ChunkStore openOrCreate(File[] dirs, String chunkName, long initialSize, StripingLocksProvider<Long> locksProvider) throws Exception {
        FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(chunkName, dirs);
        AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(factory, initialSize,
            AutoGrowingByteBufferBackedFiler.MAX_BUFFER_SEGMENT_SIZE);
        if (filer.exists()) {
            return open(filer, locksProvider);
        } else {
            return create(filer, locksProvider);
        }
    }

    public boolean checkExists(File[] dirs, String chunkName) throws IOException {
        FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(chunkName, dirs);
        return new AutoGrowingByteBufferBackedFiler(factory, 1024, AutoGrowingByteBufferBackedFiler.MAX_BUFFER_SEGMENT_SIZE).exists();
    }

    public ChunkStore open(ByteBufferFactory filer, long segmentSize, StripingLocksProvider<Long> locksProvider) throws Exception {
        return open(new AutoGrowingByteBufferBackedFiler(filer, segmentSize, segmentSize), locksProvider);
    }

    private ChunkStore open(AutoGrowingByteBufferBackedFiler filer, StripingLocksProvider<Long> locksProvider) throws Exception {
        ChunkStore chunkStore = new ChunkStore(locksProvider, filer);
        chunkStore.open();
        return chunkStore;
    }

    public ChunkStore create(ByteBufferFactory factory, long segmentSize, StripingLocksProvider<Long> locksProvider) throws Exception {
        return create(new AutoGrowingByteBufferBackedFiler(factory, segmentSize, segmentSize), locksProvider);
    }

    private ChunkStore create(AutoGrowingByteBufferBackedFiler filer, StripingLocksProvider<Long> locksProvider) throws Exception {
        ChunkStore chunkStore = new ChunkStore(locksProvider);
        chunkStore.setup(referenceNumber);
        chunkStore.createAndOpen(filer);
        return chunkStore;
    }

}
