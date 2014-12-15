package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.KeyMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bytes key to index, index to object array.
 */
public class BytesObjectMapStore<F extends ConcurrentFiler, K, V> implements KeyValueStore<K, V> {

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final V returnWhenGetReturnsNull;
    private final MapChunkProvider<F> mapChunkProvider;
    private final KeyMarshaller<K> keyMarshaller;

    public final int keySize;

    private final MapStore mapStore = MapStore.INSTANCE;
    private final AtomicReference<Object[]> payloadsRef = new AtomicReference<>();

    public BytesObjectMapStore(int keySize,
        V returnWhenGetReturnsNull,
        MapChunkProvider<F> mapChunkProvider,
        KeyMarshaller<K> keyMarshaller) {
        this.keySize = keySize;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.mapChunkProvider = mapChunkProvider;
        this.keyMarshaller = keyMarshaller;
    }

    @Override
    public void add(K key, final V value) throws IOException {
        if (key == null || value == null) {
            return;
        }

        final byte[] keyBytes = keyMarshaller.keyBytes(key);
        index(keyBytes, true, new MapTransaction<F, Void>() {
            @Override
            public Void commit(MapContext<F> chunk) throws IOException {
                final Object[] payloads = getPayloads(chunk);

                // grow the set if needed;
                if (mapStore.isFull(chunk)) {
                    int newSize = mapStore.nextGrowSize(chunk);

                    final Object[] newPayloads = new Object[mapStore.calculateCapacity(newSize)];
                    payloadsRef.set(newPayloads);
                    mapChunkProvider.grow(keyBytes, mapStore, chunk, newSize,
                        new MapStore.CopyToStream() {
                            @Override
                            public void copied(int fromIndex, int toIndex) {
                                newPayloads[toIndex] = payloads[fromIndex];
                            }
                        },
                        new MapTransaction<F, Void>() {
                            @Override
                            public Void commit(MapContext<F> chunk) throws IOException {
                                int payloadIndex = mapStore.add(chunk, (byte) 1, keyBytes, EMPTY_PAYLOAD);
                                newPayloads[payloadIndex] = value;
                                return null;
                            }
                        });
                } else {
                    int payloadIndex = mapStore.add(chunk, (byte) 1, keyBytes, EMPTY_PAYLOAD);
                    payloads[payloadIndex] = value;
                }
                return null;
            }
        });
    }

    private Object[] getPayloads(MapContext<F> chunk) throws IOException {
        Object[] payloads = payloadsRef.get();
        if (payloads == null) {
            payloads = new Object[mapStore.getCapacity(chunk)];
            payloadsRef.set(payloads);
        }
        return payloads;
    }

    @Override
    public void remove(K key) throws IOException {
        if (key == null) {
            return;
        }

        final byte[] keyBytes = keyMarshaller.keyBytes(key);
        index(keyBytes, false, new MapTransaction<F, Void>() {
            @Override
            public Void commit(MapContext<F> chunk) throws IOException {
                if (chunk != null) {
                    Object[] payloads = getPayloads(chunk);
                    int payloadIndex = mapStore.remove(chunk, keyBytes);
                    payloads[payloadIndex] = null;
                }
                return null;
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) throws IOException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        final byte[] keyBytes = keyMarshaller.keyBytes(key);
        return index(keyBytes, false, new MapTransaction<F, V>() {
            @Override
            public V commit(MapContext<F> chunk) throws IOException {
                long payloadIndex = -1;
                if (chunk != null) {
                    payloadIndex = mapStore.get(chunk, keyBytes);
                }
                if (payloadIndex < 0) {
                    return returnWhenGetReturnsNull;
                }
                Object[] payloads = getPayloads(chunk);
                return (V) payloads[(int) payloadIndex];
            }
        });
    }

    private <R> R index(byte[] pageKey, boolean createIfAbsent, MapTransaction<F, R> chunkTransaction) throws IOException {
        if (createIfAbsent) {
            return mapChunkProvider.getOrCreate(pageKey, chunkTransaction);
        } else {
            return mapChunkProvider.get(pageKey, chunkTransaction);
        }
    }

    public void copyTo(final BytesObjectMapStore<F, K, V> to) throws IOException {
        mapChunkProvider.copyTo(mapStore, to.mapChunkProvider);
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        final List<Iterator<Entry<K, V>>> iterators = Lists.newArrayList();
        try {
            mapChunkProvider.stream(mapStore, new MapTransaction<F, Void>() {
                @Override
                public Void commit(MapContext<F> chunk) throws IOException {
                    if (chunk != null) {
                        final Object[] payloads = getPayloads(chunk);
                        iterators.add(Iterators.transform(mapStore.iterator(chunk), new Function<MapStore.Entry, Entry<K, V>>() {
                            @Override
                            @SuppressWarnings("unchecked")
                            public Entry<K, V> apply(final MapStore.Entry input) {
                                final K key = keyMarshaller.bytesKey(input.key, 0);
                                final V value = (V) payloads[input.payloadIndex];

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
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<K> keysIterator() {
        final List<Iterator<K>> iterators = Lists.newArrayList();
        try {
            mapChunkProvider.stream(mapStore, new MapTransaction<F, Void>() {
                @Override
                public Void commit(MapContext<F> chunk) throws IOException {
                    if (chunk != null) {
                        iterators.add(Iterators.transform(mapStore.keysIterator(chunk), new Function<byte[], K>() {
                            @Override
                            public K apply(byte[] input) {
                                return keyMarshaller.bytesKey(input, 0);
                            }
                        }));
                    }
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Iterators.concat(iterators.iterator());
    }
}
