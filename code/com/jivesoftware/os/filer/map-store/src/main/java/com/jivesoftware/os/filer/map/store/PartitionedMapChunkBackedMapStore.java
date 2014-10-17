package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.filer.map.store.api.PartitionedKeyValueStore;
import com.jivesoftware.os.filer.map.store.extractors.ExtractIndex;
import com.jivesoftware.os.filer.map.store.extractors.ExtractKey;
import com.jivesoftware.os.filer.map.store.extractors.ExtractPayload;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author jonathan
 * @param <K>
 * @param <V>
 */
public abstract class PartitionedMapChunkBackedMapStore<K, V> implements PartitionedKeyValueStore<K, V> {

    private static final MapStore mapStore = new MapStore(ExtractIndex.SINGLETON, ExtractKey.SINGLETON, ExtractPayload.SINGLETON);

    private final MapChunkFactory chunkFactory;
    private final StripingLocksProvider<String> keyLocksProvider;
    private final Map<String, MapChunk> indexPages;
    private final V returnWhenGetReturnsNull;

    public PartitionedMapChunkBackedMapStore(MapChunkFactory chunkFactory,
            int concurrency,
            V returnWhenGetReturnsNull) {

        this.chunkFactory = chunkFactory;
        this.keyLocksProvider = new StripingLocksProvider<>(concurrency);
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.indexPages = new ConcurrentSkipListMap<>();
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
        String pageId = keyPartition(key);
        synchronized (keyLocksProvider.lock(pageId)) {
            MapChunk chunk = index(pageId);
            try {
                // grow the set if needed;
                if (mapStore.getCount(chunk) >= chunk.maxCount) {
                    int newSize = chunk.maxCount * 2;

                    chunk = chunkFactory.resize(mapStore, chunk, pageId, newSize);
                    indexPages.put(keyPartition(key), chunk);
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

        byte[] keyBytes = keyBytes(key);
        String pageId = keyPartition(key);
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
        String pageId = keyPartition(key);
        MapChunk index = index(pageId);
        byte[] keyBytes = keyBytes(key);
        byte[] payload = mapStore.get(index.duplicate(), keyBytes, ExtractPayload.SINGLETON);

        if (payload == null) {
            return returnWhenGetReturnsNull;
        }
        return bytesValue(key, payload, 0);
    }

    @Override
    public V get(K key) throws KeyValueStoreException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyBytes(key);
        byte[] payload;
        String pageId = keyPartition(key);
        synchronized (keyLocksProvider.lock(pageId)) {
            MapChunk index = index(pageId);
            payload = mapStore.get(index, keyBytes, ExtractPayload.SINGLETON);
        }
        if (payload == null) {
            return returnWhenGetReturnsNull;
        }
        return bytesValue(key, payload, 0);
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
        for (String pageId : keyPartitions()) {
            MapChunk got = get(pageId, false);
            if (got != null) {
                sizeInBytes += got.size();
            }
        }
        return sizeInBytes;
    }

    protected abstract Iterable<String> keyPartitions();

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayList();
        for (String pageId : keyPartitions()) {
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
                        final K key = bytesKey(input.key, 0);
                        final V value = bytesValue(key, input.payload, 0);

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
