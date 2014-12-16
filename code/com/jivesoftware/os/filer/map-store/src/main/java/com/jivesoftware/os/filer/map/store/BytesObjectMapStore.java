package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.KeyMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bytes key to index, index to object array.
 */
public class BytesObjectMapStore<F extends Filer, K, V> implements KeyValueStore<K, V> {

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final V returnWhenGetReturnsNull;
    private final MapChunkProvider<F> chunkProvider;
    private final KeyMarshaller<K> keyMarshaller;

    public final int keySize;

    private final MapStore mapStore = MapStore.INSTANCE;
    private final AtomicReference<Object[]> payloadsRef = new AtomicReference<>();

    public BytesObjectMapStore(int keySize,
        V returnWhenGetReturnsNull,
        MapChunkProvider<F> chunkProvider,
        KeyMarshaller<K> keyMarshaller) {
        this.keySize = keySize;
        this.returnWhenGetReturnsNull = returnWhenGetReturnsNull;
        this.chunkProvider = chunkProvider;
        this.keyMarshaller = keyMarshaller;
    }

    @Override
    public <R> R execute(final K key, boolean createIfAbsent, final KeyValueTransaction<V, R> keyValueTransaction) throws IOException {
        if (key == null) {
            return null;
        }

        final byte[] keyBytes = keyMarshaller.keyBytes(key);
        return index(keyBytes, createIfAbsent, new MapTransaction<F, R>() {
            @Override
            public R commit(final MapContext context, final F filer) throws IOException {
                if (context == null || filer == null) {
                    return keyValueTransaction.commit(null);
                } else {
                    return keyValueTransaction.commit(new KeyValueContext<V>() {
                        @Override
                        public void set(final V value) throws IOException {
                            final Object[] payloads = getPayloads(filer);

                            // grow the set if needed;
                            if (mapStore.isFull(filer, context)) {
                                int newSize = mapStore.nextGrowSize(context);
                                final Object[] newPayloads = new Object[mapStore.calculateCapacity(newSize)];
                                payloadsRef.set(newPayloads);
                                chunkProvider.grow(keyBytes, newSize,
                                    new MapStore.CopyToStream() {
                                        @Override
                                        public void copied(int fromIndex, int toIndex) {
                                            newPayloads[toIndex] = payloads[fromIndex];
                                        }
                                    },
                                    new MapTransaction<F, Void>() {
                                        @Override
                                        public Void commit(MapContext resizedContext, F resizedFiler) throws IOException {
                                            int payloadIndex = mapStore.add(resizedFiler, resizedContext, (byte) 1, keyBytes, EMPTY_PAYLOAD);
                                            newPayloads[payloadIndex] = value;
                                            return null;
                                        }
                                    });
                            } else {
                                int payloadIndex = mapStore.add(filer, context, (byte) 1, keyBytes, EMPTY_PAYLOAD);
                                payloads[payloadIndex] = value;
                            }
                        }

                        @Override
                        public void remove() throws IOException {
                            Object[] payloads = getPayloads(filer);
                            int payloadIndex = mapStore.remove(filer, context, keyBytes);
                            payloads[payloadIndex] = null;
                        }

                        @Override
                        @SuppressWarnings("unchecked")
                        public V get() throws IOException {
                            long payloadIndex = mapStore.get(filer, context, keyBytes);
                            if (payloadIndex < 0) {
                                return returnWhenGetReturnsNull;
                            }
                            Object[] payloads = getPayloads(filer);
                            return (V) payloads[(int) payloadIndex];
                        }
                    });
                }
            }
        });
    }

    private Object[] getPayloads(Filer filer) throws IOException {
        Object[] payloads = payloadsRef.get();
        if (payloads == null) {
            payloads = new Object[mapStore.getCapacity(filer)];
            payloadsRef.set(payloads);
        }
        return payloads;
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
                        final Object[] payloads = getPayloads(filer);
                        return mapStore.stream(filer, context, new MapStore.EntryStream() {
                            @Override
                            public boolean stream(MapStore.Entry entry) throws IOException {
                                K key = keyMarshaller.bytesKey(entry.key, 0);
                                @SuppressWarnings("unchecked")
                                final V value = (V) payloads[entry.payloadIndex];
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
                                K key = keyMarshaller.bytesKey(keyBytes, 0);
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
