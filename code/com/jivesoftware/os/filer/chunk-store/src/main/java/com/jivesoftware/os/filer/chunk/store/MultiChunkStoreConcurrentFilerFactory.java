package com.jivesoftware.os.filer.chunk.store;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.ConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.map.store.MapChunk;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class MultiChunkStoreConcurrentFilerFactory implements ConcurrentFilerFactory<ChunkFiler>, MultiChunkStore {

    static public class Builder {

        private final ArrayList<ChunkStore> stores = new ArrayList<>();

        private int stripingLevel = 1024;

        public Builder setStripingLevel(int stripingLevel) {
            this.stripingLevel = stripingLevel;
            return this;
        }

        public Builder addChunkStore(ChunkStore chunkStore) {
            stores.add(chunkStore);
            return this;
        }

        public MultiChunkStoreConcurrentFilerFactory build() throws IOException {
            return new MultiChunkStoreConcurrentFilerFactory(stripingLevel, stores.toArray(new ChunkStore[stores.size()]));
        }
    }

    private static final long MAGIC_SKY_HOOK_NUMBER = 5583112375L;
    private final long skyHookFP = 464; // I died a little bit doing this.

    final ChunkStore[] chunkStores;
    private final int maxKeySizePower = 16;
    private final AtomicReference<MapChunk<ChunkFiler>>[][] chunkIndexes;
    private final ByteArrayStripingLocksProvider[] locksProviders;

    private MultiChunkStoreConcurrentFilerFactory(int stripingLevel, ChunkStore... chunkStores) throws IOException {
        this.chunkStores = chunkStores;
        this.locksProviders = new ByteArrayStripingLocksProvider[chunkStores.length];
        this.chunkIndexes = new AtomicReference[chunkStores.length][maxKeySizePower];
        for (int i = 0; i < chunkStores.length; i++) {
            locksProviders[i] = new ByteArrayStripingLocksProvider(stripingLevel);
            for (int keyPower = 0; keyPower < maxKeySizePower; keyPower++) {
                chunkIndexes[i][keyPower] = new AtomicReference<>();
            }
        }
        for (ChunkStore chunkStore : chunkStores) {
            initializeIfNeeded(chunkStore);
        }
    }

    private void initializeIfNeeded(final ChunkStore chunkStore) throws IOException {
        long magic;
        try {
            ChunkFiler filer = chunkStore.getFiler(skyHookFP, new Object());
            filer.seek(0);
            magic = FilerIO.readLong(filer, "magic");
        } catch (IOException x) {
            magic = MAGIC_SKY_HOOK_NUMBER;
            long newSkyHookFP = chunkStore.newChunk(8 + (8 * maxKeySizePower));
            if (newSkyHookFP != skyHookFP) {
                throw new IOException("Its expected that the first ever allocated chunk will be at:" + skyHookFP + " but was at:" + newSkyHookFP);
            }
            try (ChunkFiler skyHookFiler = chunkStore.getFiler(newSkyHookFP, new Object())) {
                synchronized (skyHookFiler.lock()) {
                    skyHookFiler.seek(0);
                    FilerIO.writeLong(skyHookFiler, MAGIC_SKY_HOOK_NUMBER, "magic");
                    for (int i = 1, keySize = 1; i < maxKeySizePower; i++, keySize *= 2) {
                        int filerSize = MapStore.DEFAULT.computeFilerSize(2, keySize, true, 8, false);
                        long chunkFP = chunkStore.newChunk(filerSize);
                        ChunkFiler mapStoresFiler = chunkStore.getFiler(chunkFP, new Object());
                        MapStore.DEFAULT.bootstrapAllocatedFiler(2, keySize, true, 8, false, mapStoresFiler);
                        MapChunk<ChunkFiler> mapChunk = new MapChunk<>(mapStoresFiler);
                        mapChunk.init(MapStore.DEFAULT);
                        FilerIO.writeLong(skyHookFiler, mapStoresFiler.getChunkFP(), "");
                        //System.out.println("Wrote FP:" + mapStoresFiler.getChunkFP() + " for mapstore keySize:" + keySize);
                    }
                }
            }
        }
        if (magic != MAGIC_SKY_HOOK_NUMBER) {
            throw new IOException("Expected magic number:" + MAGIC_SKY_HOOK_NUMBER + " but found:" + magic);
        }
    }

    private MapChunk<ChunkFiler> growMapChunkIfNeeded(AtomicReference<MapChunk<ChunkFiler>> atomicMapChunk,
        int keyLength,
        ChunkStore chunkStore) throws IOException {

        MapChunk<ChunkFiler> mapChunk = getMapChunkIndex(atomicMapChunk, chunkStore, keyLength);
        try {
            if (MapStore.DEFAULT.isFull(mapChunk)) {
                int newSize = MapStore.DEFAULT.nextGrowSize(mapChunk);
                int chunkPower = FilerIO.chunkPower(keyLength, 1);

                int filerSize = MapStore.DEFAULT.computeFilerSize(newSize, chunkPower, true, 8, false);
                long chunkFP = chunkStore.newChunk(filerSize);
                ChunkFiler mapStoresFiler = chunkStore.getFiler(chunkFP, new Object());
                MapChunk<ChunkFiler> newMapChunk = MapStore.DEFAULT.bootstrapAllocatedFiler(newSize, chunkPower, true, 8, false, mapStoresFiler);
                MapStore.DEFAULT.copyTo(mapChunk, newMapChunk, null);

                long oldFP;
                try (ChunkFiler skyHookFiler = chunkStore.getFiler(skyHookFP, new Object())) {
                    synchronized (skyHookFiler.lock()) {
                        skyHookFiler.seek(8 + (8 * chunkPower));
                        oldFP = FilerIO.readLong(skyHookFiler, "");
                        skyHookFiler.seek(8 + (8 * chunkPower));
                        FilerIO.writeLong(skyHookFiler, mapStoresFiler.getChunkFP(), "");
                        atomicMapChunk.set(newMapChunk);
                    }
                }
                chunkStore.recycle(chunkStore.getFiler(oldFP, null));
                return newMapChunk;
            }
            return mapChunk;
        } catch (Exception e) {
            throw new IOException("Error when expanding size of partition!", e);
        }
    }

    private MapChunk<ChunkFiler> getMapChunkIndex(AtomicReference<MapChunk<ChunkFiler>> chunkIndex, ChunkStore chunkStore, int keyLength) throws IOException {

        MapChunk<ChunkFiler> mapChunk = chunkIndex.get();
        if (mapChunk == null) {
            Object lock = new Object();
            long fpIndexFP;
            try (ChunkFiler chunkFiler = chunkStore.getFiler(skyHookFP, lock)) {
                synchronized (chunkFiler.lock()) {
                    chunkFiler.seek(8 + (FilerIO.chunkPower(keyLength, 0) * 8));
                    fpIndexFP = FilerIO.readLong(chunkFiler, "mapIndexFP");
                }
            }
            ChunkFiler chunkIndexFiler = chunkStore.getFiler(fpIndexFP, lock);
            mapChunk = new MapChunk<>(chunkIndexFiler);
            mapChunk.init(MapStore.DEFAULT);
            if (!chunkIndex.compareAndSet(null, mapChunk)) {
                mapChunk = chunkIndex.get();
            }
        }
        return mapChunk;
    }

    @Override
    public ChunkFiler get(byte[] key) throws IOException {
        int i = getChunkIndexForKey(key);
        AtomicReference<MapChunk<ChunkFiler>> chunkIndex = chunkIndexes[i][FilerIO.chunkPower(key.length, 0)];
        MapChunk mapChunkIndex = getMapChunkIndex(chunkIndex, chunkStores[i], key.length);
        long ai = MapStore.DEFAULT.get(mapChunkIndex, key);
        if (ai >= 0) {
            long chunkFP = FilerIO.bytesLong(MapStore.DEFAULT.getPayload(mapChunkIndex, ai));
            if (chunkFP >= 0) {
                return chunkStores[i].getFiler(chunkFP, locksProviders[i].lock(key));
            }
        }
        return null;
    }

    @Override
    public ChunkFiler allocate(byte[] key, long size) throws IOException {
        Preconditions.checkArgument(size > 0, "Size must be positive");
        int i = getChunkIndexForKey(key);
        AtomicReference<MapChunk<ChunkFiler>> chunkIndex = chunkIndexes[i][FilerIO.chunkPower(key.length, 0)];
        MapChunk<ChunkFiler> mapChunk = getMapChunkIndex(chunkIndex, chunkStores[i], key.length);
        long chunkFP = chunkStores[i].newChunk(size);
        MapStore.DEFAULT.add(mapChunk, (byte) 1, key, FilerIO.longBytes(chunkFP));
        return chunkStores[i].getFiler(chunkFP, locksProviders[i].lock(key));
    }

    @Override
    public <R> R reallocate(byte[] key, long newSize, FilerTransaction<ChunkFiler, R> reallocateFilerTransaction) throws IOException {
        ChunkFiler oldFiler = get(key);
        if (oldFiler == null) {
            throw new IllegalStateException("Trying to reallocate an unallocated key of " + Arrays.toString(key));
        }

        ChunkFiler newFiler = allocate(key, newSize);
        R result = reallocateFilerTransaction.commit(newFiler);

        int i = getChunkIndexForKey(key);
        AtomicReference<MapChunk<ChunkFiler>> chunkIndex = chunkIndexes[i][FilerIO.chunkPower(key.length, 0)];
        //TODO MapChunk could be changing out from under us, need some locking behavior
        MapChunk mapChunk = growMapChunkIfNeeded(chunkIndex, key.length, chunkStores[i]);
        long chunkFP = newFiler.getChunkFP();
        MapStore.DEFAULT.add(mapChunk, (byte) 1, key, FilerIO.longBytes(chunkFP));

        oldFiler.recycle();
        return result;
    }

    @Override
    public void allChunks(ChunkIdStream _chunks) throws IOException {
        for (ChunkStore chunkStore : chunkStores) {
            chunkStore.allChunks(_chunks);
        }
    }

    private int getChunkIndexForKey(byte[] key) {
        return Math.abs(Arrays.hashCode(key)) % chunkStores.length;
    }

    @Override
    public long newChunk(byte[] key, long _capacity) throws IOException {
        return chunkStores[getChunkIndexForKey(key)].newChunk(_capacity);
    }

    @Override
    public <R> R execute(byte[] key, long _chunkFP, FilerTransaction<ChunkFiler, R> filerTransaction) throws IOException {
        int i = getChunkIndexForKey(key);
        Object lock = locksProviders[i].lock(key);
        try (ChunkFiler filer = chunkStores[i].getFiler(_chunkFP, lock)) {
            synchronized (filer.lock()) {
                return filerTransaction.commit(filer);
            }
        }
    }

    @Override
    public ResizingChunkFilerProvider getChunkFilerProvider(final byte[] keyBytes, final ChunkFPProvider chunkFPProvider) {

        int i = getChunkIndexForKey(keyBytes);
        final ChunkStore chunkStore = chunkStores[i];
        final Object lock = locksProviders[i].lock(keyBytes);
        return new ResizingChunkFilerProvider() {

            private final AtomicReference<ChunkFiler> filerReference = new AtomicReference<>();

            @Override
            public Object lock() {
                return lock;
            }

            @Override
            public void init(long initialChunkSize) throws IOException {
                Preconditions.checkArgument(initialChunkSize > 0);
                long chunkFP = chunkFPProvider.getChunkFP(keyBytes);
                ChunkFiler filer = null;
                if (chunkFP >= 0) {
                    filer = chunkStore.getFiler(chunkFP, lock);
                }
                if (filer == null) {
                    chunkFP = newChunk(keyBytes, initialChunkSize);
                    chunkFPProvider.setChunkFP(keyBytes, chunkFP);
                    filer = chunkStore.getFiler(chunkFP, lock);
                }
                filerReference.set(filer);
            }

            @Override
            public boolean open() throws IOException {
                ChunkFiler filer;
                long chunkFP = chunkFPProvider.getChunkFP(keyBytes);
                if (chunkFP >= 0) {
                    filer = chunkStore.getFiler(chunkFP, lock);
                } else {
                    filer = null;
                }
                filerReference.set(filer);
                return filer != null;
            }

            @Override
            public ChunkFiler get() throws IOException {
                return filerReference.get();
            }

            @Override
            public void transferTo(ResizingChunkFilerProvider to) throws IOException {
                throw new UnsupportedOperationException("Cannot transfer from ChunkFilerProvider");
            }

            @Override
            public void set(long newChunkFP) throws IOException {
                long oldChunkFP = chunkFPProvider.getAndSetChunkFP(keyBytes, newChunkFP);
                if (oldChunkFP >= 0) {
                    chunkStore.remove(oldChunkFP);
                }
            }

            @Override
            public ChunkFiler grow(long capacity) throws IOException {
                ChunkFiler currentFiler = filerReference.get();
                if (capacity >= currentFiler.length()) {
                    long currentOffset = currentFiler.getFilePointer();
                    long newChunkFP = chunkStore.newChunk(capacity);
                    ChunkFiler newFiler = chunkStore.getFiler(newChunkFP, lock);
                    copy(currentFiler, newFiler, -1);
                    long oldChunkFP = chunkFPProvider.getAndSetChunkFP(keyBytes, newChunkFP);
                    if (oldChunkFP >= 0) {
                        chunkStore.remove(oldChunkFP);
                    }
                    // copy and remove each manipulate the pointer, so restore pointer afterward
                    newFiler.seek(currentOffset);
                    filerReference.set(newFiler);
                    return newFiler;
                } else {
                    return currentFiler;
                }
            }
        };
    }

    @Override
    public ResizingChunkFilerProvider getTemporaryFilerProvider(final byte[] keyBytes) {
        int i = getChunkIndexForKey(keyBytes);
        final ChunkStore chunkStore = chunkStores[i];
        final Object lock = locksProviders[i].lock(keyBytes);

        return new ResizingChunkFilerProvider() {

            private final AtomicReference<ChunkFiler> filerReference = new AtomicReference<>();

            @Override
            public Object lock() {
                return lock;
            }

            @Override
            public void init(long initialChunkSize) throws IOException {
                Preconditions.checkArgument(initialChunkSize > 0);
                long chunkFP = newChunk(keyBytes, initialChunkSize);
                ChunkFiler filer = chunkStore.getFiler(chunkFP, lock);
                filerReference.set(filer);
            }

            @Override
            public boolean open() throws IOException {
                return true;
            }

            @Override
            public ChunkFiler get() throws IOException {
                return filerReference.get();
            }

            @Override
            public void transferTo(ResizingChunkFilerProvider toResizingChunkFilerProvider) throws IOException {
                toResizingChunkFilerProvider.set(get().getChunkFP());
                filerReference.set(null);
            }

            @Override
            public void set(long newChunkFP) throws IOException {
                throw new UnsupportedOperationException("Temporary cannot be set");
            }

            @Override
            public ChunkFiler grow(long capacity) throws IOException {
                ChunkFiler currentFiler = filerReference.get();
                if (capacity >= currentFiler.length()) {
                    long currentOffset = currentFiler.getFilePointer();
                    long newChunkFP = chunkStore.newChunk(capacity);
                    ChunkFiler newFiler = chunkStore.getFiler(newChunkFP, lock);
                    copy(currentFiler, newFiler, -1);
                    long chunkFP = currentFiler.getChunkFP();
                    chunkStore.remove(chunkFP);
                    // copy and remove each manipulate the pointer, so restore pointer afterward
                    newFiler.seek(currentOffset);

                    filerReference.set(newFiler);
                    return newFiler;
                } else {
                    return currentFiler;
                }
            }

        };
    }

    private long copy(ChunkFiler _from, ChunkFiler _to, long _bufferSize) throws IOException {
        long byteCount = _bufferSize;
        if (_bufferSize < 1) {
            byteCount = 1024 * 1024; //1MB
        }
        byte[] chunk = new byte[(int) byteCount];
        int bytesRead;
        long size = 0;
        synchronized (_from.lock()) {
            synchronized (_to.lock()) {
                _from.seek(0);
                while ((bytesRead = _from.read(chunk)) > -1) {
                    _to.seek(size);
                    _to.write(chunk, 0, bytesRead);
                    size += bytesRead;
                    _from.seek(size);
                }
                return size;
            }
        }
    }

    @Override
    public void remove(byte[] key, long _chunkFP) throws IOException {
        chunkStores[getChunkIndexForKey(key)].remove(_chunkFP);
    }

    @Override
    public void delete() throws IOException {
        for (ChunkStore chunkStore : chunkStores) {
            chunkStore.delete();
        }
    }
}
