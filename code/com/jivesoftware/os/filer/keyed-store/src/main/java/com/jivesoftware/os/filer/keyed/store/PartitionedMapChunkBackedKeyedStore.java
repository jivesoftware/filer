package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import com.jivesoftware.os.filer.map.store.PartitionedMapChunkBackedMapStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author jonathan
 */
public class PartitionedMapChunkBackedKeyedStore implements KeyedFilerStore, Copyable<PartitionedMapChunkBackedKeyedStore, Exception>,
    Iterable<KeyValueStore.Entry<IBA, SwappableFiler>> {

    private final PartitionedMapChunkBackedMapStore<IBA, IBA> mapStore;
    private final PartitionedMapChunkBackedMapStore<IBA, IBA> swapStore;
    private final MultiChunkStore chunkStore;
    private final String[] partitions;

    public PartitionedMapChunkBackedKeyedStore(MapChunkFactory mapChunkFactory,
        MapChunkFactory swapChunkFactory,
        MultiChunkStore chunkStore,
        int numPartitions)
        throws Exception {

        this.mapStore = initializeMapStore(mapChunkFactory);
        this.swapStore = initializeMapStore(swapChunkFactory);
        this.chunkStore = chunkStore;
        this.partitions = new String[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = String.valueOf(i).intern();
        }
    }

    private PartitionedMapChunkBackedMapStore<IBA, IBA> initializeMapStore(MapChunkFactory chunkFactory) throws Exception {

        // TODO push up factory!
        return new PartitionedMapChunkBackedMapStore<>(chunkFactory,
            100,
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
        AutoResizingChunkFiler filer = new AutoResizingChunkFiler(mapStore, key, chunkStore);
        if (!filer.open()) {
            if (newFilerInitialCapacity > 0) {
                filer.init(newFilerInitialCapacity);
            } else {
                return null;
            }
        }
        return new AutoResizingChunkSwappableFiler(filer, chunkStore, key, mapStore, swapStore);
    }

    @Override
    public long sizeInBytes() throws Exception {
        return mapStore.estimateSizeInBytes() + chunkStore.sizeInBytes();
    }

    public long mapStoreSizeInBytes() throws Exception {
        return mapStore.estimateSizeInBytes();
    }

    @Override
    public void copyTo(PartitionedMapChunkBackedKeyedStore to) throws Exception {
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
    public void close() {
        // TODO
    }

}
