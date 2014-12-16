package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VariableKeySizeMapChunkBackedMapStore<F extends Filer, K, V> implements KeyValueStore<K, V> {

    private final KeyValueMarshaller<K, V> keyValueMarshaller;
    private final PartitionSizedByMapStore<F>[] mapStores;
    private final V returnWhenGetReturnsNull;

    private VariableKeySizeMapChunkBackedMapStore(KeyValueMarshaller<K, V> keyValueMarshaller,
        PartitionSizedByMapStore<F>[] keyedStores,
        V returnWhenGetReturnsNull) {
        this.keyValueMarshaller = keyValueMarshaller;
        this.mapStores = keyedStores;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
    }

    private PartitionSizedByMapStore<F> getMapStore(int keyLength) {
        for (int i = 0; i < mapStores.length; i++) {
            if (mapStores[i].keySize >= keyLength) {
                return mapStores[i];
            }
        }
        throw new IndexOutOfBoundsException("Key is too long. Max supported key: " + mapStores[mapStores.length - 1].keySize + " encountred " + keyLength);
    }

    @Override
    public <R> R execute(final K key, boolean createIfAbsent, final KeyValueTransaction<V, R> keyValueTransaction) throws IOException {
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        KeyValueTransaction<byte[], R> byteTransaction = new KeyValueTransaction<byte[], R>() {
            @Override
            public R commit(final KeyValueContext<byte[]> context) throws IOException {
                return keyValueTransaction.commit(new KeyValueContext<V>() {
                    @Override
                    public void set(V value) throws IOException {
                        if (value != null) {
                            context.set(keyValueMarshaller.valueBytes(value));
                        }
                    }

                    @Override
                    public void remove() throws IOException {
                        context.remove();
                    }

                    @Override
                    public V get() throws IOException {
                        byte[] valueBytes = context.get();
                        if (valueBytes == null) {
                            return returnWhenGetReturnsNull;
                        }
                        return keyValueMarshaller.bytesValue(key, valueBytes, 0);
                    }
                });
            }
        };
        return getMapStore(keyBytes.length).store.execute(keyBytes, createIfAbsent, byteTransaction);
    }

    @Override
    public boolean stream(final EntryStream<K, V> stream) throws IOException {
        EntryStream<byte[], byte[]> byteStream = new EntryStream<byte[], byte[]>() {
            @Override
            public boolean stream(byte[] keyBytes, byte[] valueBytes) throws IOException {
                K key = keyValueMarshaller.bytesKey(keyBytes, 0);
                V value = keyValueMarshaller.bytesValue(key, valueBytes, 0);
                return stream.stream(key, value);
            }
        };
        for (PartitionSizedByMapStore<F> mapStore : mapStores) {
            if (!mapStore.store.stream(byteStream)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean streamKeys(final KeyStream<K> stream) throws IOException {
        KeyStream<byte[]> byteStream = new KeyStream<byte[]>() {
            @Override
            public boolean stream(byte[] keyBytes) throws IOException {
                K key = keyValueMarshaller.bytesKey(keyBytes, 0);
                return stream.stream(key);
            }
        };
        for (PartitionSizedByMapStore<F> mapStore : mapStores) {
            if (!mapStore.store.streamKeys(byteStream)) {
                return false;
            }
        }
        return true;
    }

    public static class Builder<F extends Filer, K, V> {

        private final V returnWhenGetReturnsNull;
        private final byte[] returnWhenGetReturnsNullBytes;
        private final KeyValueMarshaller<K, V> keyValueMarshaller;
        private final List<PartitionSizedByMapStore<F>> stores = new ArrayList<>();

        public Builder(V returnWhenGetReturnsNull, final KeyValueMarshaller<K, V> keyValueMarshaller) {
            this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
            this.keyValueMarshaller = keyValueMarshaller;
            this.returnWhenGetReturnsNullBytes = (returnWhenGetReturnsNull == null) ? null : keyValueMarshaller.valueBytes(returnWhenGetReturnsNull);
        }

        public Builder add(final int keySize, MapChunkProvider<F> mapChunkProvider) {

            stores.add(new PartitionSizedByMapStore<>(keySize,
                new PartitionedMapChunkBackedMapStore<>(mapChunkProvider,
                    returnWhenGetReturnsNullBytes,
                    PassThroughKeyValueMarshaller.INSTANCE)));
            return this;
        }

        public VariableKeySizeMapChunkBackedMapStore<F, K, V> build() throws Exception {
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

            @SuppressWarnings("unchecked")
            PartitionSizedByMapStore<F>[] storesArray = stores.toArray(new PartitionSizedByMapStore[stores.size()]);
            return new VariableKeySizeMapChunkBackedMapStore<>(keyValueMarshaller, storesArray, returnWhenGetReturnsNull);
        }

    }

    private static class PartitionSizedByMapStore<F extends Filer> implements Comparable<PartitionSizedByMapStore<F>> {

        final int keySize;
        final PartitionedMapChunkBackedMapStore<F, byte[], byte[]> store;

        public PartitionSizedByMapStore(int keySize, PartitionedMapChunkBackedMapStore<F, byte[], byte[]> store) {
            this.keySize = keySize;
            this.store = store;
        }

        @Override
        public int compareTo(PartitionSizedByMapStore<F> o) {
            return Integer.compare(keySize, o.keySize);
        }

    }
}
