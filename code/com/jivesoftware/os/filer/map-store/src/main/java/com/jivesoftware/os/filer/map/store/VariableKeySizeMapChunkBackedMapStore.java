package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class VariableKeySizeMapChunkBackedMapStore<K, V> implements KeyValueStore<K, V>, Copyable<VariableKeySizeMapChunkBackedMapStore<K, V>, Exception> {

    private final KeyValueMarshaller<K, V> keyValueMarshaller;
    private final PartitionSizedByMapStore[] mapStores;
    private final V returnWhenGetReturnsNull;

    private VariableKeySizeMapChunkBackedMapStore(KeyValueMarshaller<K, V> keyValueMarshaller,
        PartitionSizedByMapStore[] keyedStores,
        V returnWhenGetReturnsNull) {
        this.keyValueMarshaller = keyValueMarshaller;
        this.mapStores = keyedStores;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
    }

    private PartitionSizedByMapStore getMapStore(int keyLength) {
        for (int i = 0; i < mapStores.length; i++) {
            if (mapStores[i].keySize >= keyLength) {
                return mapStores[i];
            }
        }
        throw new IndexOutOfBoundsException("Key is too long. Max supported key: " + mapStores[mapStores.length - 1].keySize + " encountred " + keyLength);
    }

    @Override
    public void add(K key, V value) throws Exception {
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        byte[] valueBytes = keyValueMarshaller.valueBytes(value);
        getMapStore(keyBytes.length).store.add(keyBytes, valueBytes);
    }

    @Override
    public void remove(K key) throws Exception {
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        getMapStore(keyBytes.length).store.remove(keyBytes);
    }

    @Override
    public V get(K key) throws Exception {
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        byte[] valueBytes = getMapStore(keyBytes.length).store.get(keyBytes);
        if (valueBytes != null) {
            return keyValueMarshaller.bytesValue(key, valueBytes, 0);
        }
        return returnWhenGetReturnsNull;
    }

    @Override
    public V getUnsafe(K key) throws Exception {
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        byte[] valueBytes = getMapStore(keyBytes.length).store.getUnsafe(keyBytes);
        if (valueBytes != null) {
            return keyValueMarshaller.bytesValue(key, valueBytes, 0);
        }
        return returnWhenGetReturnsNull;
    }

    @Override
    public long estimateSizeInBytes() throws Exception {
        long sizeInBytes = 0;
        for (PartitionSizedByMapStore mapStore : mapStores) {
            sizeInBytes += mapStore.store.estimateSizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public void copyTo(VariableKeySizeMapChunkBackedMapStore<K, V> to) throws Exception {
        for (int i = 0; i < mapStores.length; i++) {
            mapStores[i].store.copyTo(to.mapStores[i].store);
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayListWithCapacity(mapStores.length);
        for (PartitionSizedByMapStore mapStore : mapStores) {
            iterators.add(Iterators.transform(mapStore.store.iterator(), new Function<Entry<byte[], byte[]>, Entry<K, V>>() {

                @Override
                public Entry<K, V> apply(final Entry<byte[], byte[]> input) {
                    final K key = keyValueMarshaller.bytesKey(input.getKey(), 0);
                    return new Entry<K, V>() {

                        @Override
                        public K getKey() {
                            return key;
                        }

                        @Override
                        public V getValue() {
                            return keyValueMarshaller.bytesValue(key, input.getValue(), 0);
                        }
                    };
                }
            }));
        }
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<K> keysIterator() {
        List<Iterator<K>> iterators = Lists.newArrayListWithCapacity(mapStores.length);
        for (PartitionSizedByMapStore mapStore : mapStores) {
            iterators.add(Iterators.transform(mapStore.store.keysIterator(), new Function<byte[], K>() {
                @Override
                public K apply(byte[] input) {
                    return keyValueMarshaller.bytesKey(input, 0);
                }
            }));
        }
        return Iterators.concat(iterators.iterator());
    }

    public static class Builder<K, V> {

        private final int concurrency;
        private final V returnWhenGetReturnsNull;
        private final byte[] returnWhenGetReturnsNullBytes;
        private final KeyValueMarshaller<K, V> keyValueMarshaller;
        private final List<PartitionSizedByMapStore> stores = new ArrayList<>();

        public Builder(int concurrency, V returnWhenGetReturnsNull, final KeyValueMarshaller<K, V> keyValueMarshaller) {
            this.concurrency = concurrency;
            this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
            this.keyValueMarshaller = keyValueMarshaller;
            this.returnWhenGetReturnsNullBytes = (returnWhenGetReturnsNull == null) ? null : keyValueMarshaller.valueBytes(returnWhenGetReturnsNull);
        }

        public Builder add(final int keySize, MapChunkFactory mapChunkFactory, final KeyPartitioner<K> keyPartitioner) {

            KeyPartitioner<byte[]> delegatingKeyPartitioner = new KeyPartitioner<byte[]>() {

                @Override
                public String keyPartition(byte[] key) {
                    return keyPartitioner.keyPartition(keyValueMarshaller.bytesKey(key, 0));
                }

                @Override
                public Iterable<String> allPartitions() {
                    return keyPartitioner.allPartitions();
                }
            };

            stores.add(new PartitionSizedByMapStore(keySize,
                new PartitionedMapChunkBackedMapStore<>(mapChunkFactory,
                    concurrency,
                    returnWhenGetReturnsNullBytes,
                    delegatingKeyPartitioner,
                    PassThroughKeyValueMarshaller.INSTANCE)));
            return this;
        }

        public VariableKeySizeMapChunkBackedMapStore<K, V> build() throws Exception {
            Collections.sort(stores);
            if (stores.isEmpty()) {
                throw new IllegalStateException("No stores were added. Must add at least one store.");
            }
            int keySize = stores.get(0).keySize;
            for (int i = 1; i < stores.size(); i++) {
                if (stores.get(i).keySize <= keySize) {
                    throw new IllegalStateException("Added stores are not monotonically orderable.");
                }
                keySize = stores.get(i).keySize;
            }

            return new VariableKeySizeMapChunkBackedMapStore<>(keyValueMarshaller,
                stores.toArray(new PartitionSizedByMapStore[stores.size()]),
                returnWhenGetReturnsNull);
        }

    }

    private static class PartitionSizedByMapStore implements Comparable<PartitionSizedByMapStore> {

        final int keySize;
        final PartitionedMapChunkBackedMapStore<byte[], byte[]> store;

        public PartitionSizedByMapStore(int keySize, PartitionedMapChunkBackedMapStore<byte[], byte[]> store) {
            this.keySize = keySize;
            this.store = store;
        }

        @Override
        public int compareTo(PartitionSizedByMapStore o) {
            return Integer.compare(keySize, o.keySize);
        }

    }
}
