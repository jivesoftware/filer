package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.chunk.store.ChunkTransaction;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
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

    private static final OpenFiler<Void, ChunkFiler> openFiler = new NoOpOpenFiler<>();
    private static final CreateFiler<Void, ChunkFiler> createFiler = new NoOpCreateFiler<>();

    private final PartitionedMapChunkBackedMapStore<F, IBA, Long> mapStore;
    private final MultiChunkStore multiChunkStore;

    public PartitionedMapChunkBackedKeyedStore(MapChunkProvider<F> mapChunkProvider,
        MultiChunkStore multiChunkStore)
        throws Exception {

        this.mapStore = initializeMapStore(mapChunkProvider);
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
            public R commit(final KeyValueContext<Long> context) throws IOException {
                if (context != null) {
                    Long chunkFP = context.get();
                    if (chunkFP == null && newFilerInitialCapacity > 0) {
                        chunkFP = multiChunkStore.newChunk(keyBytes, newFilerInitialCapacity, createFiler);
                        context.set(chunkFP);
                    }
                    if (chunkFP != null) {
                        return multiChunkStore.execute(keyBytes, chunkFP, openFiler, new ChunkTransaction<Void, R>() {
                            @Override
                            public R commit(Void monkey, ChunkFiler filer) throws IOException {
                                return transaction.commit(filer);
                                //new AutoResizingChunkFiler(multiChunkStore.getChunkFilerProvider(keyBytes, filer, context)));
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

        return mapStore.execute(new IBA(keyBytes), true, new KeyValueTransaction<Long, R>() {
            @Override
            public R commit(final KeyValueContext<Long> context) throws IOException {
                if (context != null) {
                    Long oldChunkFP = context.get();
                    if (oldChunkFP != null) {
                        return multiChunkStore.execute(keyBytes, oldChunkFP, openFiler, new ChunkTransaction<Void, R>() {
                            @Override
                            public R commit(Void monkey, final ChunkFiler oldFiler) throws IOException {
                                long newChunkFP = multiChunkStore.newChunk(keyBytes, rewriteInitialCapacity, createFiler);
                                R result = multiChunkStore.execute(keyBytes, newChunkFP, openFiler, new ChunkTransaction<Void, R>() {
                                    @Override
                                    public R commit(Void monkey, ChunkFiler newFiler) throws IOException {
                                        return transaction.commit(oldFiler, newFiler);
                                    }
                                });
                                context.set(newChunkFP);
                                return result;
                            }
                        });
                    } else {
                        long newChunkFP = multiChunkStore.newChunk(keyBytes, rewriteInitialCapacity, createFiler);
                        R result = multiChunkStore.execute(keyBytes, newChunkFP, openFiler, new ChunkTransaction<Void, R>() {
                            @Override
                            public R commit(Void monkey, ChunkFiler newFiler) throws IOException {
                                return transaction.commit(null, newFiler);
                            }
                        });
                        context.set(newChunkFP);
                        return result;
                    }
                }
                return transaction.commit(null, null);
            }
        });
    }

    @Override
    public boolean stream(final KeyValueStore.EntryStream<IBA, Filer> stream) throws IOException {
        return mapStore.stream(new KeyValueStore.EntryStream<IBA, Long>() {
            @Override
            public boolean stream(final IBA key, Long chunkFP) throws IOException {
                return multiChunkStore.execute(key.getBytes(), chunkFP, openFiler, new ChunkTransaction<Void, Boolean>() {
                    @Override
                    public Boolean commit(Void monkey, ChunkFiler filer) throws IOException {
                        return stream.stream(key, filer);
                    }
                });
            }
        });
    }

    @Override
    public boolean streamKeys(KeyValueStore.KeyStream<IBA> stream) throws IOException {
        return mapStore.streamKeys(stream);
    }

    @Override
    public void close() {
        // TODO
    }
}
