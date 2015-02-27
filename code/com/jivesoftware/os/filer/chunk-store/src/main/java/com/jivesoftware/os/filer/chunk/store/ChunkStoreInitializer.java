package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
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
        ByteBufferFactory cacheByteBufferFactory,
        int initialCacheSize,
        int maxNewCacheSize) throws Exception {

        FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(chunkName, directoryOffset, dirs);
        AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(factory, initialSize,
            AutoGrowingByteBufferBackedFiler.MAX_BUFFER_SEGMENT_SIZE);
        if (filer.exists()) {
            return open(filer, cacheByteBufferFactory, initialCacheSize, maxNewCacheSize);
        } else {
            return create(filer, cacheByteBufferFactory, initialCacheSize, maxNewCacheSize);
        }
    }

    public boolean checkExists(File[] dirs, int directoryOffset, String chunkName) throws IOException {
        FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(chunkName, directoryOffset, dirs);
        return new AutoGrowingByteBufferBackedFiler(factory, 1024, AutoGrowingByteBufferBackedFiler.MAX_BUFFER_SEGMENT_SIZE).exists();
    }

    public ChunkStore open(ByteBufferFactory filer,
        long segmentSize,
        ByteBufferFactory cacheByteBufferFactory,
        int initialCacheSize,
        int maxNewCacheSize) throws Exception {
        return open(new AutoGrowingByteBufferBackedFiler(filer, segmentSize, segmentSize), cacheByteBufferFactory, initialCacheSize, maxNewCacheSize);
    }

    private ChunkStore open(AutoGrowingByteBufferBackedFiler filer,
        ByteBufferFactory cacheByteBufferFactory,
        int initialCacheSize,
        int maxNewCacheSize) throws Exception {
        ChunkStore chunkStore = new ChunkStore(cacheByteBufferFactory, initialCacheSize, maxNewCacheSize, filer);
        chunkStore.open();
        return chunkStore;
    }

    public ChunkStore create(ByteBufferFactory factory,
        long segmentSize,
        ByteBufferFactory cacheByteBufferFactory,
        int initialCacheSize,
        int maxNewCacheSize) throws Exception {
        return create(new AutoGrowingByteBufferBackedFiler(factory, segmentSize, segmentSize), cacheByteBufferFactory, initialCacheSize, maxNewCacheSize);
    }

    private ChunkStore create(AutoGrowingByteBufferBackedFiler filer,
        ByteBufferFactory cacheByteBufferFactory,
        int initialCacheSize,
        int maxNewCacheSize) throws Exception {
        ChunkStore chunkStore = new ChunkStore(cacheByteBufferFactory, initialCacheSize, maxNewCacheSize);
        chunkStore.setup(referenceNumber);
        chunkStore.createAndOpen(filer);
        return chunkStore;
    }

}
