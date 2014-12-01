package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.KeyMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bytes key to index, index to object array.
 */
public class BytesObjectMapStore<K, V> implements KeyValueStore<K, V>, Copyable<BytesObjectMapStore<K, V>, Exception> {

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final String pageId;
    private final V returnWhenGetReturnsNull;
    private final MapChunkFactory mapChunkFactory;
    private final KeyMarshaller<K> keyMarshaller;

    public final int keySize;

    private final MapStore mapStore = MapStore.DEFAULT;
    private final AtomicReference<Index> indexRef = new AtomicReference<>();

    public BytesObjectMapStore(String pageId,
        int keySize,
        V returnWhenGetReturnsNull,
        MapChunkFactory mapChunkFactory,
        KeyMarshaller<K> keyMarshaller) {
        this.pageId = pageId;
        this.keySize = keySize;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.mapChunkFactory = mapChunkFactory;
        this.keyMarshaller = keyMarshaller;
    }

    @Override
    public void add(K key, V value) throws Exception {
        if (key == null || value == null) {
            return;
        }

        byte[] keyBytes = keyMarshaller.keyBytes(key);
        byte[] payload = EMPTY_PAYLOAD;
        synchronized (indexRef) {
            Index index = index(true);

            index = ensureCapacity(index);

            int payloadIndex = mapStore.add(index.chunk, (byte) 1, keyBytes, payload);
            /*
            System.out.println(String.format("Add key=%s value=%s index=%s capacity=%s maxCount=%s count=%s buffer=%s thread=%s",
                new String(keyBytes), System.identityHashCode(value), payloadIndex, index.chunk.capacity, index.chunk.maxCount, mapStore.getCount(index.chunk),
                System.identityHashCode(index.chunk.array), Thread.currentThread().getName()));
            */
            index.payloads[payloadIndex] = value;
        }
    }

    /*
     Must be called while holding synchronized (indexRef) lock.
     */
    private Index ensureCapacity(Index index) throws Exception {
        // grow the set if needed;
        if (mapStore.getCount(index.chunk) >= index.chunk.maxCount) {
            int newSize = index.chunk.maxCount * 2;

            final Index oldIndex = index;
            final Object[] payloads = new Object[mapStore.calculateCapacity(newSize)];
            MapChunk newChunk = mapChunkFactory.resize(mapStore, index.chunk, pageId, newSize, new MapStore.CopyToStream() {
                @Override
                public void copied(int fromIndex, int toIndex) {
                    payloads[toIndex] = oldIndex.payloads[fromIndex];
                }
            });

            index = new Index(newChunk, payloads);
            indexRef.set(index);
        }
        return index;
    }

    @Override
    public void remove(K key) throws Exception {
        if (key == null) {
            return;
        }

        byte[] keyBytes = keyMarshaller.keyBytes(key);
        synchronized (indexRef) {
            Index index = index(false);
            if (index != null) {
                int payloadIndex = mapStore.remove(index.chunk, keyBytes);
                index.payloads[payloadIndex] = null;
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) throws Exception {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyMarshaller.keyBytes(key);
        long payloadIndex = -1;
        synchronized (indexRef) {
            Index index = index(false);
            if (index != null) {
                payloadIndex = mapStore.get(index.chunk, keyBytes);
            }
            if (index == null || payloadIndex < 0) {
                return returnWhenGetReturnsNull;
            }
            return (V) index.payloads[(int) payloadIndex];
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V getUnsafe(K key) throws Exception {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyMarshaller.keyBytes(key);
        Index index = index(false);
        long payloadIndex = -1;
        if (index != null) {
            payloadIndex = mapStore.get(index.chunk.duplicate(), keyBytes);
        }
        if (index == null || payloadIndex < 0) {
            return returnWhenGetReturnsNull;
        }
        return (V) index.payloads[(int) payloadIndex];
    }

    private Index index(boolean createIfAbsent) throws Exception {
        Index got = indexRef.get();
        if (got != null) {
            return got;
        }

        synchronized (indexRef) {
            got = indexRef.get();
            if (got != null) {
                return got;
            }

            mapChunkFactory.delete(pageId);
            MapChunk mapChunk;
            if (createIfAbsent) {
                mapChunk = mapChunkFactory.getOrCreate(mapStore, pageId);
            } else {
                mapChunk = mapChunkFactory.get(mapStore, pageId);
            }
            if (mapChunk != null) {
                got = new Index(mapChunk, new Object[mapStore.getCapacity(mapChunk)]);
                indexRef.set(got);
            }
        }
        return got;
    }

    @Override
    public long estimateSizeInBytes() {
        Index index = indexRef.get();
        if (index != null) {
            return index.chunk.size() + (index.payloads.length * 8);
        }
        return 0;
    }

    @Override
    public void copyTo(BytesObjectMapStore<K, V> to) throws Exception {
        synchronized (indexRef) {
            Index got = indexRef.get();
            if (got != null) {
                to.copyFrom(got);
            }
        }
    }

    private void copyFrom(Index fromIndex) throws Exception {
        synchronized (indexRef) {
            MapChunk from = fromIndex.chunk;
            // safe just to borrow the existing payloads because indexes should be in alignment
            MapChunk to = mapChunkFactory.copy(mapStore, from, pageId, from.maxCount);
            indexRef.set(new Index(to, fromIndex.payloads));
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayList();
        final Index index;
        try {
            index = index(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (index != null) {
            MapChunk got = index.chunk;
            iterators.add(Iterators.transform(mapStore.iterator(got), new Function<MapStore.Entry, Entry<K, V>>() {
                @Override
                @SuppressWarnings("unchecked")
                public Entry<K, V> apply(final MapStore.Entry input) {
                    final K key = keyMarshaller.bytesKey(input.key, 0);
                    final V value = (V) index.payloads[input.payloadIndex];

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
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<K> keysIterator() {
        List<Iterator<K>> iterators = Lists.newArrayList();
        final Index index;
        try {
            index = index(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (index != null) {
            MapChunk got = index.chunk;
            iterators.add(Iterators.transform(mapStore.keysIterator(got), new Function<byte[], K>() {
                @Override
                public K apply(byte[] input) {
                    return keyMarshaller.bytesKey(input, 0);
                }
            }));
        }
        return Iterators.concat(iterators.iterator());
    }

    private static class Index {

        public final MapChunk chunk;
        public final Object[] payloads;

        private Index(MapChunk chunk, Object[] payloads) {
            this.chunk = chunk;
            this.payloads = payloads;
        }
    }
}
