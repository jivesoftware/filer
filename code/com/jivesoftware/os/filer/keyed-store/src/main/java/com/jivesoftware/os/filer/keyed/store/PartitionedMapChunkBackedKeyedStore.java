package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.chunk.store.ChunkTransaction;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import com.jivesoftware.os.filer.map.store.MapChunkProvider;
import com.jivesoftware.os.filer.map.store.PartitionedMapChunkBackedMapStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;

/**
 * @author jonathan
 */
public class PartitionedMapChunkBackedKeyedStore<F extends Filer> implements KeyedFilerStore {

    private final PartitionedMapChunkBackedMapStore<F, IBA, Long> mapStore;
    private final MultiChunkStore.ChunkFPProvider chunkFPProvider;
    private final MultiChunkStore multiChunkStore;

    public PartitionedMapChunkBackedKeyedStore(MapChunkProvider<F> mapChunkProvider,
        MultiChunkStore multiChunkStore)
        throws Exception {

        this.mapStore = initializeMapStore(mapChunkProvider);
        this.chunkFPProvider = new MultiChunkStore.ChunkFPProvider() {
            @Override
            public long getChunkFP(byte[] key) throws IOException {
                Long chunkFP = mapStore.get(new IBA(key));
                return chunkFP != null ? chunkFP : -1;
            }

            @Override
            public void setChunkFP(byte[] key, long chunkFP) throws IOException {
                mapStore.add(new IBA(key), chunkFP);
            }

            @Override
            public long getAndSetChunkFP(byte[] keyBytes, long newChunkFP) throws IOException {
                IBA key = new IBA(keyBytes);
                Long oldChunkFP = mapStore.get(key);
                mapStore.add(key, newChunkFP);
                return oldChunkFP != null ? oldChunkFP : -1;
            }
        };
        this.multiChunkStore = multiChunkStore;
    }

    private PartitionedMapChunkBackedMapStore<F, IBA, Long> initializeMapStore(MapChunkProvider<F> mapChunkProvider)
        throws Exception {

        // TODO push up factory!
        return new PartitionedMapChunkBackedMapStore<>(
            mapChunkProvider,
            null,
            new KeyValueMarshaller<IBA, Long>() {

                @Override
                public byte[] keyBytes(IBA key) {
                    return key.getBytes();
                }

                @Override
                public byte[] valueBytes(Long value) {
                    return FilerIO.longBytes(value);
                }

                @Override
                public IBA bytesKey(byte[] keyBytes, int offset) {
                    return new IBA(keyBytes);
                }

                @Override
                public Long bytesValue(IBA key, byte[] value, int valueOffset) {
                    return FilerIO.bytesLong(value, valueOffset);
                }
            });
    }

    @Override
    public <R> R execute(final byte[] keyBytes, final long newFilerInitialCapacity, final FilerTransaction<Filer, R> transaction) throws IOException {
        boolean createIfAbsent = (newFilerInitialCapacity > 0);
        return mapStore.execute(new IBA(keyBytes), createIfAbsent, new KeyValueTransaction<Long, R>() {
            @Override
            public R commit(KeyValueContext<Long> context) throws IOException {
                if (context != null) {
                    Long chunkFP = context.get();
                    if (chunkFP == null && newFilerInitialCapacity > 0) {
                        chunkFP = multiChunkStore.newChunk(keyBytes, newFilerInitialCapacity, new NoOpCreateFiler<ChunkFiler>());
                        context.set(chunkFP);
                    }
                    if (chunkFP != null) {
                        return multiChunkStore.execute(keyBytes, chunkFP, new NoOpOpenFiler<ChunkFiler>(), new ChunkTransaction<Void, R>() {
                            @Override
                            public R commit(Void monkey, ChunkFiler filer) throws IOException {
                                return transaction.commit(filer);
                                //TODO transaction.commit(new AutoResizingChunkFiler(multiChunkStore.getChunkFilerProvider(keyBytes, chunkFPProvider, filer)));
                            }
                        });
                    } else {
                        return transaction.commit(null);
                    }
                }
                return transaction.commit(null);
            }
        });
    }

    @Override
    public <R> R executeRewrite(final byte[] keyBytes, final long rewriteInitialCapacity, final RewriteFilerTransaction<Filer, R> transaction)
        throws IOException {

        //TODO
        return mapStore.execute(new IBA(keyBytes), true, new KeyValueTransaction<Long, R>() {
            @Override
            public R commit(KeyValueContext<Long> context) throws IOException {
                if (context != null) {
                    Long chunkFP = context.get();
                    if (chunkFP == null) {
                        chunkFP = multiChunkStore.newChunk(keyBytes, rewriteInitialCapacity, new NoOpCreateFiler<ChunkFiler>());
                        context.set(chunkFP);
                    }
                    if (chunkFP != null) {
                        return multiChunkStore.execute(keyBytes, chunkFP, new NoOpOpenFiler<ChunkFiler>(), new ChunkTransaction<Void, R>() {
                            @Override
                            public R commit(Void monkey, ChunkFiler filer) throws IOException {
                                return transaction.commit(filer);
                                //TODO transaction.commit(new AutoResizingChunkFiler(multiChunkStore.getChunkFilerProvider(keyBytes, chunkFPProvider, filer)));
                            }
                        });
                    } else {
                        return transaction.commit(null);
                    }
                }
                return transaction.commit(null, null);
            }
        });

        /*
        MultiChunkStore.ResizingChunkFilerProvider currentFilerProvider = multiChunkStore.getChunkFilerProvider(keyBytes, chunkFPProvider,
            chunkFiler);
        try(AutoResizingChunkFiler filer = new AutoResizingChunkFiler(currentFilerProvider)) {
            synchronized (filer.lock()) {
                Filer currentFiler = null;
                if (filer.open()) {
                    currentFiler = filer;
                }

                MultiChunkStore.ResizingChunkFilerProvider temporaryFilerProvider = multiChunkStore.getTemporaryFilerProvider(keyBytes);
                AutoResizingChunkFiler rewriteFiler = new AutoResizingChunkFiler(temporaryFilerProvider);
                rewriteFiler.init(rewriteInitialCapacity);

                R result = transaction.commit(currentFiler, rewriteFiler);
                temporaryFilerProvider.transferTo(currentFilerProvider);

                return result;
            }
        }
        */
    }

    @Override
    public void stream(final KeyValueStore.EntryStream<IBA, Filer> stream) throws IOException {
        mapStore.stream(new KeyValueStore.EntryStream<IBA, Long>() {
            @Override
            public boolean stream(final IBA key, Long chunkFP) throws IOException {
                return multiChunkStore.execute(key.getBytes(), chunkFP, new NoOpOpenFiler<ChunkFiler>(), new ChunkTransaction<Void, Boolean>() {
                    @Override
                    public Boolean commit(Void monkey, ChunkFiler filer) throws IOException {
                        return stream.stream(key, filer);
                    }
                });
            }
        });
    }

    @Override
    public void streamKeys(KeyValueStore.KeyStream<IBA> stream) throws IOException {
        mapStore.streamKeys(stream);
    }

    @Override
    public void close() {
        // TODO
    }
}
