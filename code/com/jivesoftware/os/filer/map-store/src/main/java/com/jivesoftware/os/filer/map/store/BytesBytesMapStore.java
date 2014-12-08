package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bytes key to bytes value.
 *
 * @param <K>
 * @param <V>
 */
public class BytesBytesMapStore<F extends ConcurrentFiler, K, V> implements KeyValueStore<K, V> {

    private final String pageId;
    private final V returnWhenGetReturnsNull;
    private final MapChunkFactory<F> mapChunkFactory;
    private final KeyValueMarshaller<K, V> keyValueMarshaller;

    public final int keySize;

    private final MapStore mapStore = MapStore.DEFAULT;
    private final AtomicReference<MapChunk<F>> indexRef = new AtomicReference<>();

    public BytesBytesMapStore(String pageId,
        int keySize,
        V returnWhenGetReturnsNull,
        MapChunkFactory<F> mapChunkFactory,
        KeyValueMarshaller<K, V> keyValueMarshaller) {

        this.pageId = pageId;
        this.keySize = keySize;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.mapChunkFactory = mapChunkFactory;
        this.keyValueMarshaller = keyValueMarshaller;
    }

    @Override
    public void add(K key, V value) throws Exception {
        if (key == null || value == null) {
            return;
        }

        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        byte[] valueBytes = keyValueMarshaller.valueBytes(value);
        if (valueBytes == null) {
            return;
        }
        synchronized (indexRef) {
            MapChunk<F> index = index(true);

            // grow the set if needed;
            if (mapStore.getCount(index) >= index.maxCount) {
                int newSize = index.maxCount * 2;

                index = mapChunkFactory.resize(mapStore, index, pageId, newSize, null);
                indexRef.set(index);
            }

            mapStore.add(index, (byte) 1, keyBytes, valueBytes);
        }
    }

    @Override
    public void remove(K key) throws Exception {
        if (key == null) {
            return;
        }

        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        synchronized (indexRef) {
            MapChunk index = index(false);
            if (index != null) {
                mapStore.remove(index, keyBytes);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) throws Exception {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        synchronized (indexRef) {
            MapChunk index = index(false);
            byte[] valueBytes = null;
            if (index != null) {
                valueBytes = mapStore.getPayload(index, keyBytes);
            }
            if (valueBytes == null) {
                return returnWhenGetReturnsNull;
            }
            return keyValueMarshaller.bytesValue(key, valueBytes, 0);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V getUnsafe(K key) throws Exception {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        MapChunk index = index(false);
        byte[] valueBytes = null;
        if (index != null) {
            valueBytes = mapStore.getPayload(index.duplicate(), keyBytes);
        }
        if (valueBytes == null) {
            return returnWhenGetReturnsNull;
        }
        return keyValueMarshaller.bytesValue(key, valueBytes, 0);
    }

    private MapChunk<F> index(boolean createIfAbsent) throws Exception {
        MapChunk<F> got = indexRef.get();
        if (got != null) {
            return got;
        }

        synchronized (indexRef) {
            got = indexRef.get();
            if (got != null) {
                return got;
            }

            if (createIfAbsent) {
                got = mapChunkFactory.getOrCreate(mapStore, pageId);
            } else {
                got = mapChunkFactory.get(mapStore, pageId);
            }
            if (got != null) {
                indexRef.set(got);
            }
        }
        return got;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayList();
        final MapChunk<F> got;
        try {
            got = index(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (got != null) {
            iterators.add(Iterators.transform(mapStore.iterator(got), new Function<MapStore.Entry, Entry<K, V>>() {
                @Override
                public Entry<K, V> apply(final MapStore.Entry input) {
                    final K key = keyValueMarshaller.bytesKey(input.key, 0);
                    final V value = keyValueMarshaller.bytesValue(key, input.payload, 0);

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

    @Override
    public Iterator<K> keysIterator() {
        List<Iterator<K>> iterators = Lists.newArrayList();
        final MapChunk got;
        try {
            got = index(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (got != null) {
            iterators.add(Iterators.transform(mapStore.keysIterator(got), new Function<byte[], K>() {
                @Override
                public K apply(byte[] input) {
                    return keyValueMarshaller.bytesKey(input, 0);
                }
            }));
        }
        return Iterators.concat(iterators.iterator());
    }
}
