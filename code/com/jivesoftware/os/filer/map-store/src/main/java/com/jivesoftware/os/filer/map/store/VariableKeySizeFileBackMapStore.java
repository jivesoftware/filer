package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.filer.map.store.api.PartitionedKeyValueStore;
import java.io.File;
import java.util.Iterator;
import java.util.List;

public abstract class VariableKeySizeFileBackMapStore<K, V> implements PartitionedKeyValueStore<K, V> {

    private final int[] keySizeThresholds;
    private final PartitionedMapChunkBackedMapStore<K, V>[] mapStores;

    @SuppressWarnings("unchecked")
    public VariableKeySizeFileBackMapStore(String[] basePathsToPartitions,
            int[] keySizeThresholds,
            int payloadSize,
            int initialPageCapacity,
            int concurrency,
            V returnWhenGetReturnsNull) {

        this.keySizeThresholds = keySizeThresholds;
        this.mapStores = new PartitionedMapChunkBackedMapStore[keySizeThresholds.length];
        for (int keySizeIndex = 0; keySizeIndex < keySizeThresholds.length; keySizeIndex++) {
            Preconditions.checkArgument(keySizeIndex == 0 || keySizeThresholds[keySizeIndex] > keySizeThresholds[keySizeIndex - 1],
                    "Thresholds must be monotonically increasing");

            final int keySize = keySizeThresholds[keySizeIndex];
            String[] pathsToPartitions = new String[basePathsToPartitions.length];
            for (int basePathIndex = 0; basePathIndex < basePathsToPartitions.length; basePathIndex++) {
                pathsToPartitions[basePathIndex] = new File(basePathsToPartitions[basePathIndex], String.valueOf(keySize)).getAbsolutePath();
            }
            
            // TODO push factory up!
            FileBackedMapChunkFactory mapChunkFactory = new FileBackedMapChunkFactory(keySize, payloadSize, initialPageCapacity, pathsToPartitions);

            mapStores[keySizeIndex]
                    = new PartitionedMapChunkBackedMapStore<K, V>(mapChunkFactory, concurrency, returnWhenGetReturnsNull) {
                        @Override
                        public String keyPartition(K key) {
                            return VariableKeySizeFileBackMapStore.this.keyPartition(key);
                        }

                        @Override
                        public Iterable<String> keyPartitions() {
                            return VariableKeySizeFileBackMapStore.this.keyPartitions(keySize);
                        }

                        @Override
                        public byte[] keyBytes(K key) {
                            byte[] keyBytes = VariableKeySizeFileBackMapStore.this.keyBytes(key);
                            byte[] padded = new byte[keySize];
                            System.arraycopy(keyBytes, 0, padded, 0, keyBytes.length);
                            return padded;
                        }

                        @Override
                        public byte[] valueBytes(V value) {
                            return VariableKeySizeFileBackMapStore.this.valueBytes(value);
                        }

                        @Override
                        public K bytesKey(byte[] bytes, int offset) {
                            return VariableKeySizeFileBackMapStore.this.bytesKey(bytes, offset);
                        }

                        @Override
                        public V bytesValue(K key, byte[] bytes, int offset) {
                            return VariableKeySizeFileBackMapStore.this.bytesValue(key, bytes, offset);
                        }
                    };
        }
    }

    private PartitionedMapChunkBackedMapStore<K, V> getMapStore(int keyLength) {
        for (int i = 0; i < keySizeThresholds.length; i++) {
            if (keySizeThresholds[i] >= keyLength) {
                return mapStores[i];
            }
        }
        throw new IndexOutOfBoundsException("Key is too long");
    }

    protected abstract int keyLength(K key);

    protected abstract Iterable<String> keyPartitions(int keyLength);

    @Override
    public void add(K key, V value) throws KeyValueStoreException {
        getMapStore(keyLength(key)).add(key, value);
    }

    @Override
    public void remove(K key) throws KeyValueStoreException {
        getMapStore(keyLength(key)).remove(key);
    }

    @Override
    public V get(K key) throws KeyValueStoreException {
        return getMapStore(keyLength(key)).get(key);
    }

    @Override
    public V getUnsafe(K key) throws KeyValueStoreException {
        return getMapStore(keyLength(key)).getUnsafe(key);
    }

    @Override
    public long estimateSizeInBytes() throws Exception {
        long sizeInBytes = 0;
        for (PartitionedMapChunkBackedMapStore<K, V> mapStore : mapStores) {
            sizeInBytes += mapStore.estimateSizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayListWithCapacity(mapStores.length);
        for (PartitionedMapChunkBackedMapStore<K, V> mapStore : mapStores) {
            iterators.add(mapStore.iterator());
        }
        return Iterators.concat(iterators.iterator());
    }
}
