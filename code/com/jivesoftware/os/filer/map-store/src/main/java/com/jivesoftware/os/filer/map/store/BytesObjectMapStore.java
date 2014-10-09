package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.filer.map.store.extractors.ExtractIndex;
import com.jivesoftware.os.filer.map.store.extractors.ExtractKey;
import com.jivesoftware.os.filer.map.store.extractors.ExtractPayload;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bytes key to index, index to object array.
 */
public abstract class BytesObjectMapStore<K, V> implements KeyValueStore<K, V> {

    private static final byte[] EMPTY_ID = new byte[16];
    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final ExtractPayload extractPayload = new ExtractPayload();
    private final ExtractIndex extractIndex = new ExtractIndex();
    private final MapStore mapStore = new MapStore(extractIndex, new ExtractKey(), extractPayload);
    private final AtomicReference<Index> indexRef = new AtomicReference<>();
    private final int keySize;
    private final int initialPageCapacity;
    private final V returnWhenGetReturnsNull;
    private final ByteBufferFactory byteBufferFactory;

    public BytesObjectMapStore(int keySize,
        int initialPageCapacity,
        V returnWhenGetReturnsNull,
        ByteBufferFactory byteBufferFactory) {
        this.keySize = keySize;
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
        byte[] payload = EMPTY_PAYLOAD;
        synchronized (indexRef) {
            Index index = index();

            index = ensureCapacity(index);

            int payloadIndex = mapStore.add(index.chunk, (byte) 1, keyBytes, payload);
            index.payloads[payloadIndex] = value;
        }
    }

    /*
     Must be called while holding synchronized (indexRef) lock.
     */
    private Index ensureCapacity(Index index) {
        // grow the set if needed;
        if (mapStore.getCount(index.chunk) >= index.chunk.maxCount) {
            int newSize = index.chunk.maxCount * 2;

            final Index oldIndex = index;
            final Index newIndex = allocate(newSize);
            mapStore.copyTo(index.chunk, newIndex.chunk, new MapStore.CopyToStream() {
                @Override
                public void copied(int fromIndex, int toIndex) {
                    newIndex.payloads[toIndex] = oldIndex.payloads[fromIndex];
                }
            });

            index = newIndex;
            indexRef.set(index);
        }
        return index;
    }

    @Override
    public void remove(K key) throws KeyValueStoreException {
        if (key == null) {
            return;
        }

        byte[] keyBytes = keyBytes(key);
        synchronized (indexRef) {
            Index index = index();
            int payloadIndex = mapStore.remove(index.chunk, keyBytes);
            index.payloads[payloadIndex] = null;
        }
    }

    @Override
    final public V bytesValue(K key, byte[] bytes, int offset) {
        return null;
    }

    @Override
    final public byte[] valueBytes(V value) {
        return EMPTY_PAYLOAD;
    }

    @Override
    @SuppressWarnings ("unchecked")
    public V get(K key) throws KeyValueStoreException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyBytes(key);
        int payloadIndex;
        synchronized (indexRef) {
            Index index = index();
            payloadIndex = mapStore.get(index.chunk, keyBytes, extractIndex);
            if (payloadIndex < 0) {
                return returnWhenGetReturnsNull;
            }
            return (V) index.payloads[payloadIndex];
        }
    }

    @SuppressWarnings ("unchecked")
    @Override
    public V getUnsafe(K key) throws KeyValueStoreException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        byte[] keyBytes = keyBytes(key);
        Index index = index();
        int payloadIndex = mapStore.get(index.chunk.duplicate(), keyBytes, extractIndex);
        if (payloadIndex < 0) {
            return returnWhenGetReturnsNull;
        }
        return (V) index.payloads[payloadIndex];
    }

    @Override
    public long estimatedMaxNumberOfKeys() {
        return mapStore.absoluteMaxCount(8, 0);
    }

    private Index index() {
        Index got = indexRef.get();
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

    private Index allocate(int maxCapacity) {
        MapChunk chunk = mapStore.allocate((byte) 0, (byte) 0, EMPTY_ID, 0, maxCapacity, keySize, 0, byteBufferFactory);
        return new Index(
            chunk,
            new Object[mapStore.getCapacity(chunk)]);
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
    public Iterator<Entry<K, V>> iterator() {
        List<Iterator<Entry<K, V>>> iterators = Lists.newArrayList();
        final Index index = index();
        if (index != null) {
            MapChunk got = index.chunk;
            iterators.add(Iterators.transform(mapStore.iterator(got), new Function<MapStore.Entry, Entry<K, V>>() {
                @Override
                @SuppressWarnings ("unchecked")
                public Entry<K, V> apply(final MapStore.Entry input) {
                    final K key = bytesKey(input.key, 0);
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

    private static class Index {

        public final MapChunk chunk;
        public final Object[] payloads;

        private Index(MapChunk chunk, Object[] payloads) {
            this.chunk = chunk;
            this.payloads = payloads;
        }
    }
}
