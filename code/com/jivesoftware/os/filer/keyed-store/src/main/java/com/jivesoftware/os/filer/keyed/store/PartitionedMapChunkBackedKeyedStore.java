package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import com.jivesoftware.os.filer.map.store.PartitionedMapChunkBackedMapStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.util.Arrays;
import java.util.Iterator;

// Key -> MapStore -> MapStore -> MultiChunks
/*
 Key -> (Strippped) -> (Stripped) -> (Stripped)
 */
/**
 * @author jonathan
 */
public class PartitionedMapChunkBackedKeyedStore<F extends ConcurrentFiler> implements KeyedFilerStore,
    Copyable<PartitionedMapChunkBackedKeyedStore<F>, Exception>,
    Iterable<KeyValueStore.Entry<IBA, SwappableFiler>> {

    private final PartitionedMapChunkBackedMapStore<F, IBA, IBA> mapStore;
    private final PartitionedMapChunkBackedMapStore<F, IBA, IBA> swapStore;
    private final MultiChunkStore chunkStore;
    private final String[] partitions;

    public PartitionedMapChunkBackedKeyedStore(MapChunkFactory<F> mapChunkFactory,
        MapChunkFactory<F> swapChunkFactory,
        MultiChunkStore chunkStore,
        StripingLocksProvider<String> keyLocksProvider,
        int numPartitions)
        throws Exception {

        this.mapStore = initializeMapStore(mapChunkFactory, keyLocksProvider);
        this.swapStore = initializeMapStore(swapChunkFactory, keyLocksProvider);
        this.chunkStore = chunkStore;
        this.partitions = new String[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = String.valueOf(i).intern();
        }
    }

    private PartitionedMapChunkBackedMapStore<F, IBA, IBA> initializeMapStore(MapChunkFactory<F> chunkFactory,
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
            new KeyValueMarshaller<IBA, IBA>() {

                @Override
                public byte[] keyBytes(IBA key) {
                    return key.getBytes();
                }

                @Override
                public byte[] valueBytes(IBA value) {
                    return value.getBytes();
                }

                @Override
                public IBA bytesKey(byte[] keyBytes, int offset) {
                    return new IBA(keyBytes);
                }

                @Override
                public IBA bytesValue(IBA key, byte[] value, int valueOffset) {
                    return new IBA(value);
                }
            });
    }

    @Override
    public SwappableFiler get(byte[] keyBytes, long newFilerInitialCapacity) throws Exception {
        IBA key = new IBA(keyBytes);
        AutoResizingChunkFiler<F> filer = new AutoResizingChunkFiler<>(mapStore, key, chunkStore);
        if (!filer.open()) {
            if (newFilerInitialCapacity > 0) {
                filer.init(newFilerInitialCapacity);
            } else {
                return null;
            }
        }
        return new AutoResizingChunkSwappableFiler<>(filer, chunkStore, key, mapStore, swapStore);
    }

    @Override
    public void copyTo(PartitionedMapChunkBackedKeyedStore<F> to) throws Exception {
        mapStore.copyTo(to.mapStore);
        swapStore.copyTo(to.swapStore);
    }

    @Override
    public Iterator<KeyValueStore.Entry<IBA, SwappableFiler>> iterator() {
        final Iterator<KeyValueStore.Entry<IBA, IBA>> iterator = mapStore.iterator();
        return new Iterator<KeyValueStore.Entry<IBA, SwappableFiler>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValueStore.Entry<IBA, SwappableFiler> next() {
                final KeyValueStore.Entry<IBA, IBA> entry = iterator.next();
                return new KeyValueStore.Entry<IBA, SwappableFiler>() {
                    @Override
                    public IBA getKey() {
                        return entry.getKey();
                    }

                    @Override
                    public SwappableFiler getValue() {
                        try {
                            return get(entry.getKey().getBytes(), 10L);
                        } catch (Exception x) {
                            throw new RuntimeException("Faield to get SwappableFiler for key:" + entry.getKey(), x);
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
