package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @param <K>
 * @param <V>
 * @author jonathan
 */
public class PartitionedMapChunkBackedMapStore<F extends ConcurrentFiler, K, V> implements KeyValueStore<K, V>,
    Copyable<PartitionedMapChunkBackedMapStore<F, K, V>, Exception> {

    private static final MapStore mapStore = MapStore.DEFAULT;

    private final MapChunkFactory<F> chunkFactory;
    private final StripingLocksProvider<String> keyLocksProvider;
    private final Map<String, MapChunk<F>> indexPages;
    private final V returnWhenGetReturnsNull;
    private final KeyPartitioner<K> keyPartitioner;
    private final KeyValueMarshaller<K, V> keyValueMarshaller;

    public PartitionedMapChunkBackedMapStore(MapChunkFactory<F> chunkFactory,
        StripingLocksProvider<String> keyLocksProvider,
        V returnWhenGetReturnsNull,
        KeyPartitioner<K> keyPartitioner,
        KeyValueMarshaller<K, V> keyValueMarshaller) {

        this.chunkFactory = chunkFactory;
        this.keyLocksProvider = keyLocksProvider;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.indexPages = new ConcurrentSkipListMap<>();
        this.keyPartitioner = keyPartitioner;
        this.keyValueMarshaller = keyValueMarshaller;
    }

    public Iterable<String> allPartitions() {
        return keyPartitioner.allPartitions();
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
        String pageId = keyPartitioner.keyPartition(key);
        synchronized (keyLocksProvider.lock(pageId)) {
            MapChunk<F> chunk = index(pageId);
            try {
                // grow the set if needed;
                if (mapStore.isFull(chunk)) {
                    int newSize = mapStore.nextGrowSize(chunk);

                    chunk = chunkFactory.resize(mapStore, chunk, pageId, newSize, null);
                    indexPages.put(pageId, chunk);
                }
            } catch (Exception e) {
                throw new KeyValueStoreException("Error when expanding size of partition!", e);
            }
            mapStore.add(chunk, (byte) 1, keyBytes, valueBytes);
        }

    }

    @Override
    public void remove(K key) throws Exception {
        if (key == null) {
            return;
        }

        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        String pageId = keyPartitioner.keyPartition(key);
        synchronized (keyLocksProvider.lock(pageId)) {
            MapChunk<F> index = get(pageId, false);
            if (index != null) {
                mapStore.remove(index, keyBytes);
            }
        }
    }

    @Override
    public V getUnsafe(K key) throws Exception {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        String pageId = keyPartitioner.keyPartition(key);
        MapChunk<F> index = get(pageId, false);
        byte[] payload = null;
        if (index != null) {
            payload = mapStore.getPayload(index.duplicate(), keyBytes);
        }
        if (payload == null) {
            return returnWhenGetReturnsNull;
        }
        return keyValueMarshaller.bytesValue(key, payload, 0);
    }

    @Override
    public V get(K key) throws Exception {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        byte[] payload = null;
        String pageId = keyPartitioner.keyPartition(key);
        synchronized (keyLocksProvider.lock(pageId)) {
            MapChunk<F> index = get(pageId, false);
            if (index != null) {
                payload = mapStore.getPayload(index, keyBytes);
            }
        }
        if (payload == null) {
            return returnWhenGetReturnsNull;
        }
        return keyValueMarshaller.bytesValue(key, payload, 0);
    }

    private MapChunk<F> index(String pageId) throws KeyValueStoreException {
        try {
            return get(pageId, true);
        } catch (Exception e) {
            throw new KeyValueStoreException("Failed to create map chunk.", e);
        }
    }

    private MapChunk<F> get(String pageId, boolean createIfAbsent) throws Exception {
        MapChunk<F> got = indexPages.get(pageId);
        if (got == null) {
            synchronized (keyLocksProvider.lock(pageId)) {
                got = indexPages.get(pageId);
                if (got == null) {
                    if (createIfAbsent) {
                        got = chunkFactory.getOrCreate(mapStore, pageId);
                    } else {
                        got = chunkFactory.get(mapStore, pageId);
                    }
                    if (got != null) {
                        indexPages.put(pageId, got);
                    }
                }
            }
        }
        return got;
    }

    @Override
    public void copyTo(PartitionedMapChunkBackedMapStore<F, K, V> to) throws Exception {
        for (String pageId : keyPartitioner.allPartitions()) {
            synchronized (keyLocksProvider.lock(pageId)) {
                MapChunk<F> got;
                try {
                    got = get(pageId, false);
                } catch (Exception x) {
                    throw new RuntimeException("Failed while loading pageId:" + pageId, x);
                }

                if (got != null) {
                    to.copyFrom(pageId, got);
                }
            }
        }
    }

    private void copyFrom(String pageId, MapChunk<F> got) throws Exception {
        synchronized (keyLocksProvider.lock(pageId)) {
            MapChunk<F> give = get(pageId, false);
            if (give != null) {
                // "resizes" the old chunk over the top of an existing chunk, using the old chunk's size
                give = chunkFactory.resize(mapStore, got, pageId, got.maxCount, null);
            } else {
                // "copies" the old chunk into a new nonexistent chunk, using the old chunk's size
                give = chunkFactory.copy(mapStore, got, pageId, got.maxCount);
            }
            indexPages.put(pageId, give);
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayList();
        for (String pageId : keyPartitioner.allPartitions()) {
            MapChunk<F> got;
            try {
                got = get(pageId, false);
            } catch (Exception x) {
                throw new RuntimeException("Failed while loading pageId:" + pageId, x);
            }

            if (got != null) {
                iterators.add(Iterators.transform(mapStore.iterator(got), new Function<MapStore.Entry, Entry<K, V>>() {
                    @Override
                    public Entry<K, V> apply(final MapStore.Entry input) {
                        final K key = keyValueMarshaller.bytesKey(input.key, 0);

                        return new Entry<K, V>() {
                            V value;

                            @Override
                            public K getKey() {
                                return key;
                            }

                            @Override
                            public V getValue() {
                                if (value == null) {
                                    value = keyValueMarshaller.bytesValue(key, input.payload, 0);
                                }
                                return value;
                            }
                        };
                    }
                }));
            }
        }
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<K> keysIterator() {
        List<Iterator<K>> iterators = Lists.newArrayList();
        for (String pageId : keyPartitioner.allPartitions()) {
            MapChunk<F> got;
            try {
                got = get(pageId, false);
            } catch (Exception x) {
                throw new RuntimeException("Failed while loading pageId:" + pageId, x);
            }

            if (got != null) {
                iterators.add(Iterators.transform(mapStore.keysIterator(got), new Function<byte[], K>() {
                    @Override
                    public K apply(byte[] input) {
                        return keyValueMarshaller.bytesKey(input, 0);
                    }
                }));
            }
        }
        return Iterators.concat(iterators.iterator());
    }
}
