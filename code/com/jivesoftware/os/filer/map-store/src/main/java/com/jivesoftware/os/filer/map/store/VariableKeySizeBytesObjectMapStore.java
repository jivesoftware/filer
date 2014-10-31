package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.KeyMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import java.util.Iterator;
import java.util.List;

public class VariableKeySizeBytesObjectMapStore<K, V> implements KeyValueStore<K, V>, Copyable<VariableKeySizeBytesObjectMapStore<K, V>, Exception> {

    private final int[] keySizeThresholds;
    private final KeyMarshaller<K> keyMarshaller;
    private final BytesObjectMapStore<byte[], V>[] mapStores;

    @SuppressWarnings("unchecked")
    public VariableKeySizeBytesObjectMapStore(int[] keySizeThresholds,
        int initialPageCapacity,
        V returnWhenGetReturnsNull,
        ByteBufferProvider byteBufferProvider,
        final KeyMarshaller<K> keyMarshaller) {

        this.keySizeThresholds = keySizeThresholds;
        this.keyMarshaller = keyMarshaller;
        this.mapStores = new BytesObjectMapStore[keySizeThresholds.length];

        PassThroughKeyMarshaller passThroughKeyMarshaller = PassThroughKeyMarshaller.INSTANCE;
        for (int i = 0; i < keySizeThresholds.length; i++) {
            Preconditions.checkArgument(i == 0 || keySizeThresholds[i] > keySizeThresholds[i - 1], "Thresholds must be monotonically increasing");

            final int keySize = keySizeThresholds[i];
            mapStores[i] = new BytesObjectMapStore<>(
                keySize, true, initialPageCapacity, returnWhenGetReturnsNull, byteBufferProvider, passThroughKeyMarshaller);
        }
    }

    private BytesObjectMapStore<byte[], V> getMapStore(int keyLength) {
        for (int i = 0; i < keySizeThresholds.length; i++) {
            if (keySizeThresholds[i] >= keyLength) {
                return mapStores[i];
            }
        }
        throw new IndexOutOfBoundsException("Key is too long");
    }

    public void add(K key, V value) throws KeyValueStoreException {
        byte[] keyBytes = keyMarshaller.keyBytes(key);
        getMapStore(keyBytes.length).add(keyBytes, value);
    }

    @Override
    public void remove(K key) throws KeyValueStoreException {
        byte[] keyBytes = keyMarshaller.keyBytes(key);
        getMapStore(keyBytes.length).remove(keyBytes);
    }

    @Override
    public V get(K key) throws KeyValueStoreException {
        byte[] keyBytes = keyMarshaller.keyBytes(key);
        return getMapStore(keyBytes.length).get(keyBytes);
    }

    @Override
    public V getUnsafe(K key) throws KeyValueStoreException {
        byte[] keyBytes = keyMarshaller.keyBytes(key);
        return getMapStore(keyBytes.length).getUnsafe(keyBytes);
    }

    @Override
    public long estimateSizeInBytes() throws Exception {
        long estimate = 0;
        for (BytesObjectMapStore<byte[], V> mapStore : mapStores) {
            estimate += mapStore.estimateSizeInBytes();
        }
        return estimate;
    }

    @Override
    public void copyTo(VariableKeySizeBytesObjectMapStore<K, V> to) throws Exception {
        for (int i = 0; i < mapStores.length; i++) {
            mapStores[i].copyTo(to.mapStores[i]);
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayListWithCapacity(mapStores.length);
        for (BytesObjectMapStore<byte[], V> mapStore : mapStores) {
            iterators.add(Iterators.transform(mapStore.iterator(), new Function<Entry<byte[], V>, Entry<K, V>>() {
                @Override
                public Entry<K, V> apply(final Entry<byte[], V> input) {
                    return new Entry<K, V>() {

                        private K key = keyMarshaller.bytesKey(input.getKey(), 0);

                        @Override
                        public K getKey() {
                            return key;
                        }

                        @Override
                        public V getValue() {
                            return input.getValue();
                        }
                    };
                }
            }));
        }
        return Iterators.concat(iterators.iterator());
    }
}