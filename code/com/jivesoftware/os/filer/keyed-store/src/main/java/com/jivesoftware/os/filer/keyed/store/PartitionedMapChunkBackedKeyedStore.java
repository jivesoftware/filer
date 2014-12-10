package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import com.jivesoftware.os.filer.map.store.PartitionedMapChunkBackedMapStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author jonathan
 */
public class PartitionedMapChunkBackedKeyedStore<F extends ConcurrentFiler> implements KeyedFilerStore,
    Copyable<PartitionedMapChunkBackedKeyedStore<F>>,
    Iterable<KeyValueStore.Entry<IBA, Filer>> {

    private final PartitionedMapChunkBackedMapStore<F, IBA, Long> mapStore;
    private final MultiChunkStore.ChunkFPProvider chunkFPProvider;
    private final MultiChunkStore multiChunkStore;
    private final String[] partitions;

    public PartitionedMapChunkBackedKeyedStore(MapChunkFactory<F> mapChunkFactory,
        MultiChunkStore multiChunkStore,
        StripingLocksProvider<String> keyLocksProvider,
        int numPartitions)
        throws Exception {

        this.mapStore = initializeMapStore(mapChunkFactory, keyLocksProvider);
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
        this.partitions = new String[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = String.valueOf(i).intern();
        }
    }

    private PartitionedMapChunkBackedMapStore<F, IBA, Long> initializeMapStore(MapChunkFactory<F> chunkFactory,
        final StripingLocksProvider<String> keyLocksProvider)
        throws Exception {

        // TODO push up factory!
        return new PartitionedMapChunkBackedMapStore<>(chunkFactory,
            keyLocksProvider,
            null,
            new KeyPartitioner<IBA>() {
                @Override
                public String keyPartition(IBA key) {
                    return partitions[Math.abs(key.hashCode()) % partitions.length];
                }

                @Override
                public Iterable<String> allPartitions() {
                    return Arrays.asList(partitions);
                }
            },
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
    public <R> R execute(byte[] keyBytes, long newFilerInitialCapacity, FilerTransaction<Filer, R> transaction) throws IOException {
        try (AutoResizingChunkFiler filer = new AutoResizingChunkFiler(multiChunkStore.getChunkFilerProvider(keyBytes, chunkFPProvider))) {
            synchronized (filer.lock()) {
                if (!filer.open()) {
                    if (newFilerInitialCapacity > 0) {
                        filer.init(newFilerInitialCapacity);
                    } else {
                        return transaction.commit(null);
                    }
                }
                return transaction.commit(filer);
            }
        }
    }

    @Override
    public <R> R executeRewrite(byte[] keyBytes, long rewriteInitialCapacity, RewriteFilerTransaction<Filer, R> transaction) throws IOException {
        MultiChunkStore.ResizingChunkFilerProvider currentFilerProvider = multiChunkStore.getChunkFilerProvider(keyBytes, chunkFPProvider);
        try (AutoResizingChunkFiler filer = new AutoResizingChunkFiler(currentFilerProvider)) {
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
    }

    @Override
    public void copyTo(PartitionedMapChunkBackedKeyedStore<F> to) throws IOException {
        mapStore.copyTo(to.mapStore);
    }

    @Override
    public Iterator<KeyValueStore.Entry<IBA, Filer>> iterator() {
        final Iterator<IBA> iterator = mapStore.keysIterator();
        return new Iterator<KeyValueStore.Entry<IBA, Filer>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValueStore.Entry<IBA, Filer> next() {
                final IBA next = iterator.next();
                return new KeyValueStore.Entry<IBA, Filer>() {
                    @Override
                    public IBA getKey() {
                        return next;
                    }

                    @Override
                    public Filer getValue() {
                        try {
                            return execute(next.getBytes(), -1, new FilerTransaction<Filer, Filer>() {
                                @Override
                                public Filer commit(Filer filer) throws IOException {
                                    return filer;
                                }
                            });
                        } catch (Exception x) {
                            throw new RuntimeException("Failed to get Filer for key:" + next, x);
                        }
                    }
                };
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Not supported.");
            }
        };
    }

    @Override
    public Iterator<IBA> keysIterator() {
        return mapStore.keysIterator();
    }

    @Override
    public void close() {
        // TODO
    }
}
