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

    public ChunkStore openOrCreate(File[] dirs,
        int directoryOffset,
        String chunkName,
        long initialSize,
        ByteBufferFactory locksFactory,
        StripingLocksProvider<Long> locksProvider) throws Exception {

        FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(chunkName, directoryOffset, dirs);
        AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(factory, initialSize,
            AutoGrowingByteBufferBackedFiler.MAX_BUFFER_SEGMENT_SIZE);
        if (filer.exists()) {
            return open(filer, locksFactory, locksProvider);
        } else {
            return create(filer, locksFactory, locksProvider);
        }
    }

    public boolean checkExists(File[] dirs, int directoryOffset, String chunkName) throws IOException {
        FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(chunkName, directoryOffset, dirs);
        return new AutoGrowingByteBufferBackedFiler(factory, 1024, AutoGrowingByteBufferBackedFiler.MAX_BUFFER_SEGMENT_SIZE).exists();
    }

    public ChunkStore open(ByteBufferFactory filer,
        long segmentSize,
        ByteBufferFactory locksFactory,
        StripingLocksProvider<Long> locksProvider) throws Exception {
        return open(new AutoGrowingByteBufferBackedFiler(filer, segmentSize, segmentSize), locksFactory, locksProvider);
    }

    private ChunkStore open(AutoGrowingByteBufferBackedFiler filer,
        ByteBufferFactory locksFactory,
        StripingLocksProvider<Long> locksProvider) throws Exception {
        ChunkStore chunkStore = new ChunkStore(new ChunkLocks(locksFactory), locksProvider, filer);
        chunkStore.open();
        return chunkStore;
    }

    public ChunkStore create(ByteBufferFactory factory,
        long segmentSize,
        ByteBufferFactory locksFactory,
        StripingLocksProvider<Long> locksProvider) throws Exception {
        return create(new AutoGrowingByteBufferBackedFiler(factory, segmentSize, segmentSize), locksFactory, locksProvider);
    }

    private ChunkStore create(AutoGrowingByteBufferBackedFiler filer,
        ByteBufferFactory locksFactory,
        StripingLocksProvider<Long> locksProvider) throws Exception {
        ChunkStore chunkStore = new ChunkStore(new ChunkLocks(locksFactory), locksProvider);
        chunkStore.setup(referenceNumber);
        chunkStore.createAndOpen(filer);
        return chunkStore;
    }

}
