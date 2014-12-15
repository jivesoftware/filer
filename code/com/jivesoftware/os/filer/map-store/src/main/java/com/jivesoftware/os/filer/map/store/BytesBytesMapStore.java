package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Bytes key to bytes value.
 *
 * @param <K>
 * @param <V>
 */
public class BytesBytesMapStore<F extends ConcurrentFiler, K, V> implements KeyValueStore<K, V> {

    private final V returnWhenGetReturnsNull;
    private final MapChunkProvider<F> mapChunkProvider;
    private final KeyValueMarshaller<K, V> keyValueMarshaller;

    public final int keySize;

    private final MapStore mapStore = MapStore.INSTANCE;

    public BytesBytesMapStore(int keySize,
        V returnWhenGetReturnsNull,
        MapChunkProvider<F> mapChunkProvider,
        KeyValueMarshaller<K, V> keyValueMarshaller) {

        this.keySize = keySize;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.mapChunkProvider = mapChunkProvider;
        this.keyValueMarshaller = keyValueMarshaller;
    }

    @Override
    public void add(K key, V value) throws IOException {
        if (key == null || value == null) {
            return;
        }

        final byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        final byte[] valueBytes = keyValueMarshaller.valueBytes(value);
        if (valueBytes == null) {
            return;
        }
        index(keyBytes, true, new MapTransaction<F, Void>() {
            @Override
            public Void commit(MapContext<F> index) throws IOException {
                // grow the set if needed;
                if (mapStore.isFull(index)) {
                    int newSize = mapStore.nextGrowSize(index);
                    mapChunkProvider.grow(keyBytes, mapStore, index, newSize, null, new MapTransaction<F, Void>() {
                        @Override
                        public Void commit(MapContext<F> resizedIndex) throws IOException {
                            mapStore.add(resizedIndex, (byte) 1, keyBytes, valueBytes);
                            return null;
                        }
                    });
                } else {
                    mapStore.add(index, (byte) 1, keyBytes, valueBytes);
                }
                return null;
            }
        });
    }

    @Override
    public void remove(K key) throws IOException {
        if (key == null) {
            return;
        }

        final byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        index(keyBytes, false, new MapTransaction<F, Void>() {
            @Override
            public Void commit(MapContext<F> index) throws IOException {
                if (index != null) {
                    mapStore.remove(index, keyBytes);
                }
                return null;
            }
        });

    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(final K key) throws IOException {
        if (key == null) {
            return returnWhenGetReturnsNull;
        }
        final byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        return index(keyBytes, false, new MapTransaction<F, V>() {
            @Override
            public V commit(MapContext<F> index) throws IOException {
                byte[] valueBytes = null;
                if (index != null) {
                    valueBytes = mapStore.getPayload(index, keyBytes);
                }
                if (valueBytes == null) {
                    return returnWhenGetReturnsNull;
                }
                return keyValueMarshaller.bytesValue(key, valueBytes, 0);
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

    /**
     * Not thread safe. Lock externally. GOOD LUCK!
     */
    @Override
    public Iterator<Entry<K, V>> iterator() {
        final List<Iterator<Entry<K, V>>> iterators = Lists.newArrayList();
        try {
            mapChunkProvider.stream(mapStore, new MapTransaction<F, Void>() {
                @Override
                public Void commit(MapContext<F> filer) throws IOException {
                    if (filer != null) {
                        iterators.add(Iterators.transform(mapStore.iterator(filer), new Function<MapStore.Entry, Entry<K, V>>() {
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
                    return null;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Iterators.concat(iterators.iterator());
    }

    /**
     * Not thread safe. Lock externally. GOOD LUCK!
     */
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
                                return keyValueMarshaller.bytesKey(input, 0);
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
