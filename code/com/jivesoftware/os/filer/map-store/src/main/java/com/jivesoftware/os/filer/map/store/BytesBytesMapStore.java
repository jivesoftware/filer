package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.filer.map.store.api.PartitionedKeyValueStore;
import com.jivesoftware.os.filer.map.store.extractors.ExtractIndex;
import com.jivesoftware.os.filer.map.store.extractors.ExtractKey;
import com.jivesoftware.os.filer.map.store.extractors.ExtractPayload;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bytes key to bytes value.
 * @param <K>
 * @param <V>
 */
public abstract class BytesBytesMapStore<K, V> implements PartitionedKeyValueStore<K, V> {

    private static final byte[] EMPTY_ID = new byte[16];

    private final MapStore mapStore = MapStore.DEFAULT;
    private final AtomicReference<MapChunk> indexRef = new AtomicReference<>();
    private final int keySize;
    private final int payloadSize;
    private final int initialPageCapacity;
    private final V returnWhenGetReturnsNull;
    private final ByteBufferFactory byteBufferFactory;

    public BytesBytesMapStore(int keySize,
        int payloadSize,
        int initialPageCapacity,
        V returnWhenGetReturnsNull,
        ByteBufferFactory byteBufferFactory) {
        this.keySize = keySize;
        this.payloadSize = payloadSize;
        this.initialPageCapacity = initialPageCapacity;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.byteBufferFactory = byteBufferFactory;
    }

    @Override
    public void add(K key, V value) throws KeyValueStoreException {
        if (key == null || value == null) {
            return;
        }

        byte[] keyBytes = keyBytes(key);
        byte[] valueBytes = valueBytes(value);
        if (valueBytes == null) {
            return;
        }
        synchronized (indexRef) {
            MapChunk index = index();

            // grow the set if needed;
            if (mapStore.getCount(index) >= index.maxCount) {
                int newSize = index.maxCount * 2;

                final MapChunk oldIndex = index;
                final MapChunk newIndex = allocate(newSize);
                mapStore.copyTo(oldIndex, newIndex, null);

                index = newIndex;
                indexRef.set(index);
            }

            mapStore.add(index, (byte) 1, keyBytes, valueBytes);
        }
    }

    @Override
    public void remove(K key) throws KeyValueStoreException {
        if (key == null) {
            return;
        }

        byte[] keyBytes = keyBytes(key);
        synchronized (indexRef) {
            MapChunk index = index();
            mapStore.remove(index, keyBytes);
        }
    }

    @Override
    public String keyPartition(K key) {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) throws KeyValueStoreException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyBytes(key);
        synchronized (indexRef) {
            MapChunk index = index();
            byte[] valueBytes = mapStore.get(index, keyBytes, mapStore.extractPayload);
            if (valueBytes == null) {
                return returnWhenGetReturnsNull;
            }
            return bytesValue(key, valueBytes, 0);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V getUnsafe(K key) throws KeyValueStoreException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyBytes(key);
        MapChunk index = index();
        byte[] valueBytes = mapStore.get(index.duplicate(), keyBytes, mapStore.extractPayload);
        if (valueBytes == null) {
            return returnWhenGetReturnsNull;
        }
        return bytesValue(key, valueBytes, 0);
    }

    private MapChunk index() {
        MapChunk got = indexRef.get();
        if (got != null) {
            return got;
        }

        synchronized (indexRef) {
            got = indexRef.get();
            if (got != null) {
                return got;
            }

            got = allocate(initialPageCapacity);
            indexRef.set(got);
        }
        return got;
    }

    private MapChunk allocate(int maxCapacity) {
        return mapStore.allocate((byte) 0, (byte) 0, EMPTY_ID, 0, maxCapacity, keySize, payloadSize,
            byteBufferFactory);
    }

    @Override
    public long estimateSizeInBytes() {
        MapChunk mapChunk = indexRef.get();
        if (mapChunk != null) {
            return mapChunk.size();
        }
        return 0;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayList();
        final MapChunk got = index();
        if (got != null) {
            iterators.add(Iterators.transform(mapStore.iterator(got), new Function<MapStore.Entry, Entry<K, V>>() {
                @Override
                public Entry<K, V> apply(final MapStore.Entry input) {
                    final K key = bytesKey(input.key, 0);
                    final V value = bytesValue(key, input.payload, 0);

                    return new Entry<K, V>() {
                        @Override
                        public K getKey() {
                            return key;
                        }

                        @Override
                        @SuppressWarnings("unchecked")
                        public V getValue() {
                            return value;
                        }
                    };
                }
            }));
        }
        return Iterators.concat(iterators.iterator());
    }
}
