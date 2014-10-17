package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore.Entry;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.filer.map.store.extractors.ExtractIndex;
import com.jivesoftware.os.filer.map.store.extractors.ExtractKey;
import com.jivesoftware.os.filer.map.store.extractors.ExtractPayload;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bytes key to index, index to byte array offset.
 */
public abstract class ByteBufferValueArrayMapStore<K, V> implements KeyValueStore<K, V> {

    private static final byte[] EMPTY_ID = new byte[16];
    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final ExtractPayload extractPayload = ExtractPayload.SINGLETON;
    private final ExtractIndex extractIndex = ExtractIndex.SINGLETON;
    private final MapStore mapStore = new MapStore(extractIndex, ExtractKey.SINGLETON, extractPayload);
    private final AtomicReference<Index> indexRef = new AtomicReference<>();
    private final int keySize;
    private final int payloadSize;
    private final int initialPageCapacity;
    private final V returnWhenGetReturnsNull;
    private final HeapByteBufferFactory byteBufferFactory;

    public ByteBufferValueArrayMapStore(int keySize,
            int payloadSize,
            int initialPageCapacity,
            V returnWhenGetReturnsNull,
            HeapByteBufferFactory byteBufferFactory) {
        this.keySize = keySize;
        this.payloadSize = payloadSize;
        this.initialPageCapacity = initialPageCapacity;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.byteBufferFactory = byteBufferFactory;
    }

    @Override
    public long estimateSizeInBytes() {
        Index index = indexRef.get();
        if (index != null) {
            return index.chunk.size() + index.payloads.capacity();
        }
        return -1;
    }

    @Override
    public void add(final K key, V value) throws KeyValueStoreException {
        if (key == null || value == null) {
            return;
        }

        byte[] keyBytes = keyBytes(key);
        byte[] valueBytes = valueBytes(value);
        if (valueBytes == null) {
            return;
        }
        synchronized (indexRef) {
            Index index = index();

            // grow the set if needed;
            if (mapStore.getCount(index.chunk) >= index.chunk.maxCount) {
                int newSize = index.chunk.maxCount * 2;

                final Index oldIndex = index;
                final Index newIndex = allocate(newSize);
                mapStore.copyTo(index.chunk, newIndex.chunk, new MapStore.CopyToStream() {
                    @Override
                    public void copied(int fromIndex, int toIndex) {
                        byte[] fromBytes = oldIndex.getValue(fromIndex, payloadSize);
                        newIndex.putValue(fromBytes, toIndex, payloadSize);
                    }
                });

                index = newIndex;
                indexRef.set(index);
            }

            int payloadIndex = mapStore.add(index.chunk, (byte) 1, keyBytes, EMPTY_PAYLOAD);
            putValue(index, payloadIndex, value);
        }
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
            putValue(index, payloadIndex, null);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
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
            return getValue(index, key, payloadIndex);
        }
    }

    @SuppressWarnings("unchecked")
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
        return getValue(index, key, payloadIndex);
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
        MapChunk chunk = mapStore.allocate((byte) 0, (byte) 0, EMPTY_ID, 0, maxCapacity, keySize, 0,
                byteBufferFactory);
        return new Index(
                chunk,
                allocateValue(mapStore.getCapacity(chunk)));
    }

    public long sizeInBytes() throws IOException {
        Index index = indexRef.get();
        if (index != null) {
            return index.chunk.size();
        } else {
            return 0;
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        Index index = index();
        if (index != null) {
            final Index dupe = new Index(index.chunk.duplicate(), index.payloads.duplicate());
            return Iterators.transform(mapStore.iterator(dupe.chunk), new Function<MapStore.Entry, Entry<K, V>>() {
                @Override
                public Entry<K, V> apply(final MapStore.Entry input) {
                    final K key = bytesKey(input.key, 0);
                    final V value = getValue(dupe, key, input.payloadIndex);

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
            });
        } else {
            return Iterators.emptyIterator();
        }
    }

    private V getValue(Index index, K key, int offset) {
        byte[] valueBytes = index.getValue(offset, payloadSize);
        return bytesValue(key, valueBytes, 0);
    }

    private void putValue(Index index, int offset, V value) {
        byte[] valueBytes = value != null ? valueBytes(value) : new byte[payloadSize];
        index.putValue(valueBytes, offset, payloadSize);
    }

    private ByteBuffer allocateValue(int size) {
        return ByteBuffer.allocate(size * payloadSize);
    }

    private static class Index {

        public final MapChunk chunk;
        public final ByteBuffer payloads;

        private Index(MapChunk chunk, ByteBuffer payloads) {
            this.chunk = chunk;
            this.payloads = payloads;
        }

        private byte[] getValue(int offset, int payloadSize) {
            byte[] valueBytes = new byte[payloadSize];
            payloads.position(offset * payloadSize);
            payloads.get(valueBytes, 0, payloadSize);
            return valueBytes;
        }

        private void putValue(byte[] valueBytes, int offset, int payloadSize) {
            payloads.position(offset * payloadSize);
            payloads.put(valueBytes, 0, payloadSize);
        }
    }
}
