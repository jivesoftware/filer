package com.jivesoftware.os.filer.chunk.store;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.ConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.MonkeyFilerTransaction;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.RewriteMonkeyFilerTransaction;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import com.jivesoftware.os.filer.map.store.MapTransaction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class MultiChunkStoreConcurrentFilerFactory implements ConcurrentFilerFactory<ChunkFiler>, MultiChunkStore {

    static public class Builder {

        private final ArrayList<ChunkStore> stores = new ArrayList<>();

        private ByteArrayStripingLocksProvider locksProvider;

        public Builder setLocksProvider(ByteArrayStripingLocksProvider locksProvider) {
            this.locksProvider = locksProvider;
            return this;
        }

        public Builder addChunkStore(ChunkStore chunkStore) {
            stores.add(chunkStore);
            return this;
        }

        public MultiChunkStoreConcurrentFilerFactory build() throws IOException {
            Preconditions.checkArgument(!stores.isEmpty(), "Must add at least one ChunkStore");
            return new MultiChunkStoreConcurrentFilerFactory(
                Preconditions.checkNotNull(locksProvider, "Must set a LocksProvider"),
                stores.toArray(new ChunkStore[stores.size()]));
        }
    }

    private static final long MAGIC_SKY_HOOK_NUMBER = 5583112375L;
    private static final NoOpOpenFiler<ChunkFiler> noOpOpenChunkFiler = new NoOpOpenFiler<>();

    private static final long skyHookFP = 464; // I died a little bit doing this.

    final ChunkStore[] chunkStores;
    private final int maxKeySizePower = 16;
    private final AtomicLong[][] chunkIndexes;
    private final ByteArrayStripingLocksProvider locksProvider;
    private final Object skyHookLock = new Object();

    private MultiChunkStoreConcurrentFilerFactory(ByteArrayStripingLocksProvider locksProvider, ChunkStore... chunkStores) throws IOException {
        this.locksProvider = locksProvider;
        this.chunkStores = chunkStores;
        this.chunkIndexes = new AtomicLong[chunkStores.length][maxKeySizePower];
        for (int i = 0; i < chunkStores.length; i++) {
            for (int keyPower = 0; keyPower < maxKeySizePower; keyPower++) {
                chunkIndexes[i][keyPower] = new AtomicLong(-1);
            }
        }
        for (ChunkStore chunkStore : chunkStores) {
            initializeIfNeeded(chunkStore);
        }
    }

    private void initializeIfNeeded(final ChunkStore chunkStore) throws IOException {
        long magic;
        try {
            magic = chunkStore.execute(skyHookFP, noOpOpenChunkFiler,
                new ChunkTransaction<Void, Long>() {
                    @Override
                    public Long commit(Void monkey, ChunkFiler filer) throws IOException {
                        filer.seek(0);
                        return FilerIO.readLong(filer, "magic");
                    }
                }
            );
        } catch (IOException x) {
            magic = MAGIC_SKY_HOOK_NUMBER;
            chunkStore.newChunk(8 + (8 * maxKeySizePower), new CreateFiler<Void, ChunkFiler>() {
                @Override
                public Void create(final ChunkFiler skyHookFiler) throws IOException {
                    long newSkyHookFP = skyHookFiler.getChunkFP();

                    if (newSkyHookFP != skyHookFP) {
                        throw new IOException("Its expected that the first ever allocated chunk will be at:" + skyHookFP + " but was at:" + newSkyHookFP);
                    }

                    skyHookFiler.seek(0);
                    FilerIO.writeLong(skyHookFiler, MAGIC_SKY_HOOK_NUMBER, "magic");
                    for (int i = 1, keySize = 1; i < maxKeySizePower; i++, keySize *= 2) {
                        int filerSize = MapStore.INSTANCE.computeFilerSize(2, keySize, true, 8, false);
                        final int _keySize = keySize;
                        chunkStore.newChunk(filerSize, new CreateFiler<MapContext, ChunkFiler>() {
                            @Override
                            public MapContext create(ChunkFiler mapStoresFiler) throws IOException {
                                MapContext chunk = MapStore.INSTANCE.create(2, _keySize, true, 8, false, mapStoresFiler);
                                FilerIO.writeLong(skyHookFiler, mapStoresFiler.getChunkFP(), "");
                                return chunk;
                            }
                        });
                    }
                    return null;
                }
            });

        }
        if (magic != MAGIC_SKY_HOOK_NUMBER) {
            throw new IOException("Expected magic number:" + MAGIC_SKY_HOOK_NUMBER + " but found:" + magic);
        }
    }

    private final OpenFiler<MapContext, ChunkFiler> openMapContext = new OpenFiler<MapContext, ChunkFiler>() {
        @Override
        public MapContext open(ChunkFiler filer) throws IOException {
            return MapStore.INSTANCE.open(filer);
        }
    };

    /**
     * Lock chunkIndex externally!
     */
    private <R> R updateChunkIndex(final AtomicLong chunkIndex,
        final int keyLength,
        final ChunkStore chunkStore,
        final MapTransaction<ChunkFiler, R> filerTransaction) throws IOException {

        synchronized (chunkIndex) {
            long fpChunkFP = getMapChunkFP(chunkIndex, chunkStore, keyLength);
            try {
                return chunkStore.execute(fpChunkFP,
                    openMapContext,
                    new ChunkTransaction<MapContext, R>() {
                        @Override
                        public R commit(final MapContext currentIndexChunk, final ChunkFiler currentIndexFiler) throws IOException {
                            if (MapStore.INSTANCE.isFull(currentIndexFiler, currentIndexChunk)) {
                                final int newSize = MapStore.INSTANCE.nextGrowSize(currentIndexChunk);
                                final int chunkPower = FilerIO.chunkPower(keyLength, 1);
                                int filerSize = MapStore.INSTANCE.computeFilerSize(newSize, chunkPower, true, 8, false);

                                final long chunkFP = chunkStore.newChunk(filerSize, new CreateFiler<MapContext, ChunkFiler>() {
                                    @Override
                                    public MapContext create(ChunkFiler newIndexFiler) throws IOException {
                                        MapContext newIndexChunk = MapStore.INSTANCE.create(newSize, keyLength, true, 8, false, newIndexFiler);
                                        MapStore.INSTANCE.copyTo(currentIndexFiler, currentIndexChunk, newIndexFiler, newIndexChunk, null);
                                        return newIndexChunk;
                                    }
                                });

                                long oldFP;
                                synchronized (skyHookLock) {
                                    oldFP = chunkStore.execute(skyHookFP, noOpOpenChunkFiler,
                                        new ChunkTransaction<Void, Long>() {
                                            @Override
                                            public Long commit(Void monkey, ChunkFiler skyHookFiler) throws IOException {
                                                skyHookFiler.seek(8 + (8 * chunkPower));
                                                long oldFP = FilerIO.readLong(skyHookFiler, "");
                                                skyHookFiler.seek(8 + (8 * chunkPower));
                                                FilerIO.writeLong(skyHookFiler, chunkFP, "");
                                                chunkIndex.set(chunkFP);
                                                return oldFP;
                                            }
                                        });
                                }

                                chunkStore.remove(oldFP);

                                return chunkStore.execute(chunkFP, openMapContext,
                                    new ChunkTransaction<MapContext, R>() {
                                        @Override
                                        public R commit(MapContext chunk, ChunkFiler filer) throws IOException {
                                            return filerTransaction.commit(chunk, filer);
                                        }
                                    });
                            } else {
                                return filerTransaction.commit(currentIndexChunk, currentIndexFiler);
                            }
                        }
                    });
            } catch (Exception e) {
                throw new IOException("Error when expanding size of partition!", e);
            }
        }
    }

    /**
     * Lock chunkIndex externally!
     */
    private long getMapChunkFP(AtomicLong chunkIndex, final ChunkStore chunkStore, final int keyLength) throws IOException {
        long fpIndexFP = chunkIndex.get();
        if (fpIndexFP == -1) {
            fpIndexFP = chunkStore.execute(skyHookFP, noOpOpenChunkFiler, new ChunkTransaction<Void, Long>() {
                @Override
                public Long commit(Void monkey, ChunkFiler chunkFiler) throws IOException {
                    chunkFiler.seek(8 + (FilerIO.chunkPower(keyLength, 0) * 8));
                    return FilerIO.readLong(chunkFiler, "mapIndexFP");
                }
            });
            chunkIndex.set(fpIndexFP);
        }
        return fpIndexFP;
    }

    @Override
    public <M, R> R getOrAllocate(final byte[] key,
        final long size,
        final OpenFiler<M, ChunkFiler> openFiler,
        final CreateFiler<M, ChunkFiler> createFiler,
        final MonkeyFilerTransaction<M, ChunkFiler, R> filerTransaction)
        throws IOException {

        Preconditions.checkArgument(size > 0, "Size must be positive");
        int i = getChunkIndexForKey(key);
        AtomicLong chunkIndex = chunkIndexes[i][FilerIO.chunkPower(key.length, 0)];
        Object lock = locksProvider.lock(key);
        synchronized (lock) {
            long chunkFP;
            final ChunkStore chunkStore = chunkStores[i];
            synchronized (chunkIndex) {
                long fpChunkFP = getMapChunkFP(chunkIndex, chunkStore, key.length);
                chunkFP = chunkStore.execute(fpChunkFP, openMapContext, new ChunkTransaction<MapContext, Long>() {
                    @Override
                    public Long commit(MapContext mapContext, ChunkFiler filer) throws IOException {
                        long ai = MapStore.INSTANCE.get(filer, mapContext, key);
                        if (ai >= 0) {
                            return FilerIO.bytesLong(MapStore.INSTANCE.getPayload(filer, mapContext, ai));
                        } else if (size > 0) {
                            long chunkFP = chunkStore.newChunk(size, createFiler);
                            MapStore.INSTANCE.add(filer, mapContext, (byte) 1, key, FilerIO.longBytes(chunkFP));
                            return chunkFP;
                        } else {
                            return -1L;
                        }
                    }
                });
            }
            if (chunkFP >= 0) {
                return chunkStore.execute(chunkFP, openFiler, new ChunkTransaction<M, R>() {
                    @Override
                    public R commit(M monkey, ChunkFiler filer) throws IOException {
                        return filerTransaction.commit(monkey, filer);
                    }
                });
            } else {
                return filerTransaction.commit(null, null);
            }
        }
    }

    @Override
    public void delete(final byte[] key) throws IOException {
        int i = getChunkIndexForKey(key);
        ChunkStore chunkStore = chunkStores[i];
        AtomicLong chunkIndex = chunkIndexes[i][FilerIO.chunkPower(key.length, 0)];
        Object lock = locksProvider.lock(key);
        synchronized (lock) {
            synchronized (chunkIndex) {
                long mapChunkFP = getMapChunkFP(chunkIndex, chunkStore, key.length);
                long chunkFP = chunkStore.execute(mapChunkFP, openMapContext, new ChunkTransaction<MapContext, Long>() {
                    @Override
                    public Long commit(MapContext mapChunk, ChunkFiler filer) throws IOException {
                        long ai = MapStore.INSTANCE.get(filer, mapChunk, key);
                        long chunkFP = -1;
                        if (ai >= 0) {
                            chunkFP = FilerIO.bytesLong(MapStore.INSTANCE.getPayload(filer, mapChunk, ai));
                            MapStore.INSTANCE.remove(filer, mapChunk, key);
                        }
                        return chunkFP;
                    }
                });
                chunkStore.remove(chunkFP);
            }
        }
    }

    @Override
    public <M, R> R grow(final byte[] key,
        final long newSize,
        final OpenFiler<M, ChunkFiler> openFiler,
        final CreateFiler<M, ChunkFiler> createFiler,
        final RewriteMonkeyFilerTransaction<M, ChunkFiler, R> filerTransaction) throws IOException {

        int i = getChunkIndexForKey(key);
        final ChunkStore chunkStore = chunkStores[i];
        final AtomicLong chunkIndex = chunkIndexes[i][FilerIO.chunkPower(key.length, 0)];
        return getOrAllocate(key, -1, openFiler, createFiler, new MonkeyFilerTransaction<M, ChunkFiler, R>() {
            @Override
            public R commit(final M oldMonkey, final ChunkFiler oldFiler) throws IOException {
                if (oldFiler == null) {
                    throw new IllegalStateException("Trying to grow an unallocated key of " + Arrays.toString(key));
                }

                final long newChunkFP = chunkStore.newChunk(newSize, createFiler);
                return chunkStore.execute(newChunkFP, openFiler, new ChunkTransaction<M, R>() {
                    @Override
                    public R commit(M newMonkey, ChunkFiler newFiler) throws IOException {
                        R result = filerTransaction.commit(oldMonkey, oldFiler, newMonkey, newFiler);

                        updateChunkIndex(chunkIndex, key.length, chunkStore, new MapTransaction<ChunkFiler, Void>() {
                            @Override
                            public Void commit(MapContext mapContext, ChunkFiler filer) throws IOException {
                                MapStore.INSTANCE.add(filer, mapContext, (byte) 1, key, FilerIO.longBytes(newChunkFP));
                                return null;
                            }
                        });

                        oldFiler.recycle();
                        return result;
                    }
                });
            }
        });
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
    public <M> long newChunk(byte[] key, long _capacity, CreateFiler<M, ChunkFiler> createFiler) throws IOException {
        return chunkStores[getChunkIndexForKey(key)].newChunk(_capacity, createFiler);
    }

    @Override
    public <M, R> R execute(byte[] key, long chunkFP, OpenFiler<M, ChunkFiler> openFiler, ChunkTransaction<M, R> chunkTransaction) throws IOException {
        return chunkStores[getChunkIndexForKey(key)].execute(chunkFP, openFiler, chunkTransaction);
    }

    @Override
    public void remove(byte[] key, long _chunkFP) throws IOException {
        chunkStores[getChunkIndexForKey(key)].remove(_chunkFP);
    }

    @Override
    public ResizingChunkFilerProvider getChunkFilerProvider(final byte[] keyBytes, final ChunkFPProvider chunkFPProvider, final ChunkFiler chunkFiler) {
        int i = getChunkIndexForKey(keyBytes);
        final ChunkStore chunkStore = chunkStores[i];
        final Object lock = locksProvider.lock(keyBytes);
        final AtomicReference<ChunkFiler> filerReference = new AtomicReference<>(chunkFiler);
        return new ResizingChunkFilerProvider() {

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
                    long newChunkFP = chunkStore.newChunk(capacity, lock, createChunk);
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
        final Object lock = locksProvider.lock(keyBytes);

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
    public void delete() throws IOException {
        for (ChunkStore chunkStore : chunkStores) {
            chunkStore.delete();
        }
    }
}
