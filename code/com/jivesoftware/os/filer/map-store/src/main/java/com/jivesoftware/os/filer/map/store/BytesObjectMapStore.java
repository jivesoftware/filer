package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.KeyMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bytes key to index, index to object array.
 */
public class BytesObjectMapStore<F extends ConcurrentFiler, K, V> implements KeyValueStore<K, V>, Copyable<BytesObjectMapStore<F, K, V>, Exception> {

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final String pageId;
    private final V returnWhenGetReturnsNull;
    private final MapChunkFactory<F> mapChunkFactory;
    private final KeyMarshaller<K> keyMarshaller;

    public final int keySize;

    private final MapStore mapStore = MapStore.DEFAULT;
    private final AtomicReference<Index<F>> indexRef = new AtomicReference<>();

    public BytesObjectMapStore(String pageId,
        int keySize,
        V returnWhenGetReturnsNull,
        MapChunkFactory<F> mapChunkFactory,
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
            Index<F> index = index(true);

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
    private Index<F> ensureCapacity(Index<F> index) throws Exception {
        // grow the set if needed;
        if (mapStore.getCount(index.chunk) >= index.chunk.maxCount) {
            int newSize = index.chunk.maxCount * 2;

            final Index<F> oldIndex = index;
            final Object[] payloads = new Object[mapStore.calculateCapacity(newSize)];
            MapChunk<F> newChunk = mapChunkFactory.resize(mapStore, index.chunk, pageId, newSize, new MapStore.CopyToStream() {
                @Override
                public void copied(int fromIndex, int toIndex) {
                    payloads[toIndex] = oldIndex.payloads[fromIndex];
                }
            });

            index = new Index<>(newChunk, payloads);
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
            Index<F> index = index(false);
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
            Index<F> index = index(false);
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
        Index<F> index = index(false);
        long payloadIndex = -1;
        if (index != null) {
            payloadIndex = mapStore.get(index.chunk.duplicate(), keyBytes);
        }
        if (index == null || payloadIndex < 0) {
            return returnWhenGetReturnsNull;
        }
        return (V) index.payloads[(int) payloadIndex];
    }

    private Index<F> index(boolean createIfAbsent) throws Exception {
        Index<F> got = indexRef.get();
        if (got != null) {
            return got;
        }

        synchronized (indexRef) {
            got = indexRef.get();
            if (got != null) {
                return got;
            }

            mapChunkFactory.delete(pageId);
            MapChunk<F> mapChunk;
            if (createIfAbsent) {
                mapChunk = mapChunkFactory.getOrCreate(mapStore, pageId);
            } else {
                mapChunk = mapChunkFactory.get(mapStore, pageId);
            }
            if (mapChunk != null) {
                got = new Index<>(mapChunk, new Object[mapStore.getCapacity(mapChunk)]);
                indexRef.set(got);
            }
        }
        return got;
    }

    @Override
    public void copyTo(BytesObjectMapStore<F, K, V> to) throws Exception {
        synchronized (indexRef) {
            Index<F> got = indexRef.get();
            if (got != null) {
                to.copyFrom(got);
            }
        }
    }

    private void copyFrom(Index<F> fromIndex) throws Exception {
        synchronized (indexRef) {
            MapChunk<F> from = fromIndex.chunk;
            // safe just to borrow the existing payloads because indexes should be in alignment
            MapChunk<F> to = mapChunkFactory.copy(mapStore, from, pageId, from.maxCount);
            indexRef.set(new Index<>(to, fromIndex.payloads));
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayList();
        final Index<F> index;
        try {
            index = index(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (index != null) {
            MapChunk<F> got = index.chunk;
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
        final Index<F> index;
        try {
            index = index(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (index != null) {
            MapChunk<F> got = index.chunk;
            iterators.add(Iterators.transform(mapStore.keysIterator(got), new Function<byte[], K>() {
                @Override
                public K apply(byte[] input) {
                    return keyMarshaller.bytesKey(input, 0);
                }
            }));
        }
        return Iterators.concat(iterators.iterator());
    }

    private static class Index<F extends ConcurrentFiler> {

        public final MapChunk<F> chunk;
        public final Object[] payloads;

        private Index(MapChunk<F> chunk, Object[] payloads) {
            this.chunk = chunk;
            this.payloads = payloads;
        }
    }
}
