package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;

/**
 * Bytes key to bytes value.
 *
 * @param <K>
 * @param <V>
 */
public class BytesBytesMapStore<F extends Filer, K, V> implements KeyValueStore<K, V> {

    private final V returnWhenGetReturnsNull;
    private final MapChunkProvider<F> chunkProvider;
    private final KeyValueMarshaller<K, V> keyValueMarshaller;

    public final int keySize;

    private final MapStore mapStore = MapStore.INSTANCE;

    public BytesBytesMapStore(int keySize,
        V returnWhenGetReturnsNull,
        MapChunkProvider<F> chunkProvider,
        KeyValueMarshaller<K, V> keyValueMarshaller) {

        this.keySize = keySize;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.chunkProvider = chunkProvider;
        this.keyValueMarshaller = keyValueMarshaller;
    }

    @Override
    public <R> R execute(final K key, boolean createIfAbsent, final KeyValueTransaction<V, R> keyValueTransaction) throws IOException {
        if (key == null) {
            return null;
        }

        final byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        return index(keyBytes, createIfAbsent, new MapTransaction<F, R>() {
            @Override
            public R commit(final MapContext context, final F filer) throws IOException {
                if (context == null || filer == null) {
                    return keyValueTransaction.commit(null);
                } else {
                    return keyValueTransaction.commit(new KeyValueContext<V>() {
                        @Override
                        public void set(V value) throws IOException {
                            if (value == null) {
                                return;
                            }
                            final byte[] valueBytes = keyValueMarshaller.valueBytes(value);
                            if (valueBytes == null) {
                                return;
                            }
                            // grow the set if needed;
                            if (mapStore.isFull(filer, context)) {
                                int newSize = mapStore.nextGrowSize(context);
                                chunkProvider.grow(keyBytes, newSize, null, new MapTransaction<F, Void>() {
                                    @Override
                                    public Void commit(MapContext resizedContext, F filer) throws IOException {
                                        mapStore.add(filer, resizedContext, (byte) 1, keyBytes, valueBytes);
                                        return null;
                                    }
                                });
                            } else {
                                mapStore.add(filer, context, (byte) 1, keyBytes, valueBytes);
                            }
                        }

                        @Override
                        public void remove() throws IOException {
                            mapStore.remove(filer, context, keyBytes);
                        }

                        @Override
                        public V get() throws IOException {
                            byte[] payload = mapStore.getPayload(filer, context, keyBytes);
                            if (payload == null) {
                                return returnWhenGetReturnsNull;
                            }
                            return keyValueMarshaller.bytesValue(key, payload, 0);
                        }
                    });
                }
            }
        });
    }

    private <R> R index(byte[] pageKey, boolean createIfAbsent, MapTransaction<F, R> chunkTransaction) throws IOException {
        if (createIfAbsent) {
            return chunkProvider.getOrCreate(pageKey, chunkTransaction);
        } else {
            return chunkProvider.get(pageKey, chunkTransaction);
        }
    }

    @Override
    public boolean stream(final EntryStream<K, V> stream) throws IOException {
        try {
            return chunkProvider.stream(new MapTransaction<F, Boolean>() {
                @Override
                public Boolean commit(MapContext context, F filer) throws IOException {
                    if (context != null && filer != null) {
                        return mapStore.stream(filer, context, new MapStore.EntryStream() {
                            @Override
                            public boolean stream(MapStore.Entry entry) throws IOException {
                                K key = keyValueMarshaller.bytesKey(entry.key, 0);
                                V value = keyValueMarshaller.bytesValue(key, entry.payload, 0);
                                return stream.stream(key, value);
                            }
                        });
                    }
                    return true;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean streamKeys(final KeyStream<K> stream) throws IOException {
        try {
            return chunkProvider.stream(new MapTransaction<F, Boolean>() {
                @Override
                public Boolean commit(MapContext context, F filer) throws IOException {
                    if (context != null && filer != null) {
                        return mapStore.streamKeys(filer, context, new MapStore.KeyStream() {
                            @Override
                            public boolean stream(byte[] keyBytes) throws IOException {
                                K key = keyValueMarshaller.bytesKey(keyBytes, 0);
                                return stream.stream(key);
                            }
                        });
                    }
                    return true;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
