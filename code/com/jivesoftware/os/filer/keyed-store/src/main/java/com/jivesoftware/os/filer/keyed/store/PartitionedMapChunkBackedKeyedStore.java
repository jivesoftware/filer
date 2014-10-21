package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import com.jivesoftware.os.filer.map.store.PartitionedMapChunkBackedMapStore;
import java.util.Arrays;

/**
 * @author jonathan
 */
public class PartitionedMapChunkBackedKeyedStore implements KeyedFilerStore {

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
        if (newFilerInitialCapacity <= 0 && !filer.exists()) {
            return null;
        }
        filer.init(newFilerInitialCapacity);
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
    public void close() {
        // TODO
    }
}
