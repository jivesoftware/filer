package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
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
public class PartitionedMapChunkBackedMapStore<K, V> implements KeyValueStore<K, V>, Copyable<PartitionedMapChunkBackedMapStore<K, V>, Exception> {

    private static final MapStore mapStore = MapStore.DEFAULT;

    private final MapChunkFactory chunkFactory;
    private final StripingLocksProvider<String> keyLocksProvider;
    private final Map<String, MapChunk> indexPages;
    private final V returnWhenGetReturnsNull;
    private final KeyPartitioner<K> keyPartitioner;
    private final KeyValueMarshaller<K, V> keyValueMarshaller;

    public PartitionedMapChunkBackedMapStore(MapChunkFactory chunkFactory,
        int concurrency,
        V returnWhenGetReturnsNull,
        KeyPartitioner<K> keyPartitioner,
        KeyValueMarshaller<K, V> keyValueMarshaller) {

        this.chunkFactory = chunkFactory;
        this.keyLocksProvider = new StripingLocksProvider<>(concurrency);
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.indexPages = new ConcurrentSkipListMap<>();
        this.keyPartitioner = keyPartitioner;
        this.keyValueMarshaller = keyValueMarshaller;
    }

    public Iterable<String> allPartitions() {
        return keyPartitioner.allPartitions();
    }

    @Override
    public void add(K key, V value) throws KeyValueStoreException {
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
            MapChunk chunk = index(pageId);
            try {
                // grow the set if needed;
                if (mapStore.getCount(chunk) >= chunk.maxCount) {
                    int newSize = chunk.maxCount * 2;

                    chunk = chunkFactory.resize(mapStore, chunk, pageId, newSize);
                    indexPages.put(pageId, chunk);
                }
            } catch (Exception e) {
                throw new KeyValueStoreException("Error when expanding size of partition!", e);
            }
            mapStore.add(chunk, (byte) 1, keyBytes, valueBytes);
        }

    }

    @Override
    public void remove(K key) throws KeyValueStoreException {
        if (key == null) {
            return;
        }

        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        String pageId = keyPartitioner.keyPartition(key);
        synchronized (keyLocksProvider.lock(pageId)) {
            MapChunk index = index(pageId);
            mapStore.remove(index, keyBytes);
        }
    }

    @Override
    public V getUnsafe(K key) throws KeyValueStoreException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        String pageId = keyPartitioner.keyPartition(key);
        MapChunk index = index(pageId);
        byte[] payload = mapStore.getPayload(index.duplicate(), keyBytes);
        if (payload == null) {
            return returnWhenGetReturnsNull;
        }
        return keyValueMarshaller.bytesValue(key, payload, 0);
    }

    @Override
    public V get(K key) throws KeyValueStoreException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        byte[] payload;
        String pageId = keyPartitioner.keyPartition(key);
        synchronized (keyLocksProvider.lock(pageId)) {
            MapChunk index = index(pageId);
            payload = mapStore.getPayload(index, keyBytes);
        }
        if (payload == null) {
            return returnWhenGetReturnsNull;
        }
        return keyValueMarshaller.bytesValue(key, payload, 0);
    }

    private MapChunk index(String pageId) throws KeyValueStoreException {
        try {
            return get(pageId, true);
        } catch (Exception e) {
            throw new KeyValueStoreException("Failed to create map chunk.", e);
        }
    }

    private MapChunk get(String pageId, boolean createIfAbsent) throws Exception {
        MapChunk got = indexPages.get(pageId);
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
    public long estimateSizeInBytes() throws Exception {
        long sizeInBytes = 0;
        for (String pageId : keyPartitioner.allPartitions()) {
            MapChunk got = get(pageId, false);
            if (got != null) {
                sizeInBytes += got.size();
            }
        }
        return sizeInBytes;
    }

    @Override
    public void copyTo(PartitionedMapChunkBackedMapStore<K, V> to) throws Exception {
        for (String pageId : keyPartitioner.allPartitions()) {
            synchronized (keyLocksProvider.lock(pageId)) {
                MapChunk got;
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

    private void copyFrom(String pageId, MapChunk got) throws Exception {
        synchronized (keyLocksProvider.lock(pageId)) {
            MapChunk give = get(pageId, false);
            if (give != null) {
                // "resizes" the old chunk over the top of an existing chunk, using the old chunk's size
                give = chunkFactory.resize(mapStore, got, pageId, got.maxCount);
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
            MapChunk got;
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
                        final V value = keyValueMarshaller.bytesValue(key, input.payload, 0);

                        return new Entry<K, V>() {
                            @Override
                            public K getKey() {
                                return key;
                            }

                            @Override
                            public V getValue() {
                                return value;
                            }
                        };
                    }
                }));
            }
        }
        return Iterators.concat(iterators.iterator());
    }
}
