/*
 * Copyright 2014 Jive Software.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkTransaction;
import com.jivesoftware.os.filer.chunk.store.transaction.MapCreator;
import com.jivesoftware.os.filer.chunk.store.transaction.MapGrower;
import com.jivesoftware.os.filer.chunk.store.transaction.MapOpener;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMap;
import com.jivesoftware.os.filer.chunk.store.transaction.TxPartitionedNamedMap;
import com.jivesoftware.os.filer.chunk.store.transaction.TxStream;
import com.jivesoftware.os.filer.io.ByteArrayPartitionFunction;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;
import java.util.List;

/**
 * @param <K>
 * @param <V>
 * @author jonathan.colt
 */
public class TxKeyValueStore<K, V> implements KeyValueStore<K, V> {

    static final long SKY_HOOK_FP = 464; // I died a little bit doing this.
    private final KeyValueMarshaller<K, V> keyValueMarshaller;
    private final byte[] name;
    private final TxPartitionedNamedMap namedMap;

    public TxKeyValueStore(ChunkStore[] chunkStores, KeyValueMarshaller<K, V> keyValueMarshaller, byte[] name, int keySize, boolean variableKeySize,
        int payloadSize, boolean variablePayloadSize) {
        this.keyValueMarshaller = keyValueMarshaller;
        this.name = name;

        // TODO consider replacing with builder pattern
        TxNamedMap[] stores = new TxNamedMap[chunkStores.length];
        for (int i = 0; i < stores.length; i++) {
            stores[i] = new TxNamedMap(chunkStores[i], SKY_HOOK_FP,
                new MapCreator(2, keySize, variableKeySize, payloadSize, variablePayloadSize),
                MapOpener.INSTANCE,
                new MapGrower<>(1));
        }

        this.namedMap = new TxPartitionedNamedMap(ByteArrayPartitionFunction.INSTANCE, stores);
    }

    @Override
    public boolean[] contains(List<K> keys) throws IOException {
        byte[][] keysBytes = new byte[keys.size()][];
        for (int i = 0; i < keysBytes.length; i++) {
            keysBytes[i] = keyValueMarshaller.keyBytes(keys.get(i));
        }
        return namedMap.contains(keysBytes, name);
    }

    @Override
    public <R> R execute(final K key, boolean createIfAbsent, final KeyValueTransaction<V, R> keyValueTransaction) throws IOException {
        final byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        if (createIfAbsent) {
            return namedMap.write(keyBytes, name, new ChunkTransaction<MapContext, R>() {

                @Override
                public R commit(final MapContext monkey, final ChunkFiler filer, final Object lock) throws IOException {
                    return keyValueTransaction.commit(new KeyValueContext<V>() {

                        @Override
                        public void set(V value) throws IOException {
                            synchronized (lock) {
                                MapStore.INSTANCE.add(filer, monkey, (byte) 1, keyBytes, keyValueMarshaller.valueBytes(value));
                            }
                        }

                        @Override
                        public void remove() throws IOException {
                            synchronized (lock) {
                                MapStore.INSTANCE.remove(filer, monkey, keyBytes);
                            }
                        }

                        @Override
                        public V get() throws IOException {
                            synchronized (lock) {
                                long pi = MapStore.INSTANCE.get(filer, monkey, keyBytes);
                                if (pi > -1) {
                                    byte[] rawValue = MapStore.INSTANCE.getPayload(filer, monkey, pi);
                                    return keyValueMarshaller.bytesValue(key, rawValue, 0);
                                }
                                return null;
                            }
                        }
                    });
                }
            });
        } else {
            return namedMap.read(keyBytes, name, new ChunkTransaction<MapContext, R>() {

                @Override
                public R commit(final MapContext monkey, final ChunkFiler filer, final Object lock) throws IOException {
                    return keyValueTransaction.commit(new KeyValueContext<V>() {

                        @Override
                        public void set(V value) throws IOException {
                            throw new IllegalStateException("cannot set within a read tx.");
                        }

                        @Override
                        public void remove() throws IOException {
                            if (filer != null && monkey != null) {
                                synchronized (lock) {
                                    MapStore.INSTANCE.remove(filer, monkey, keyBytes);
                                }
                            }
                        }

                        @Override
                        public V get() throws IOException {
                            if (filer != null && monkey != null) {
                                synchronized (lock) {
                                    long pi = MapStore.INSTANCE.get(filer, monkey, keyBytes);
                                    if (pi > -1) {
                                        byte[] rawValue = MapStore.INSTANCE.getPayload(filer, monkey, pi);
                                        return keyValueMarshaller.bytesValue(key, rawValue, 0);
                                    }
                                }
                            }
                            return null;
                        }
                    });
                }
            });
        }
    }

    @Override
    public boolean stream(final EntryStream<K, V> stream) throws IOException {
        return namedMap.stream(name, new TxStream<byte[], MapContext, ChunkFiler>() {

            @Override
            public boolean stream(byte[] key, MapContext monkey, ChunkFiler filer, Object lock) throws IOException {
                if (monkey == null || filer == null) {
                    return true;
                }
                return MapStore.INSTANCE.stream(filer, monkey, lock, new MapStore.EntryStream() {

                    @Override
                    public boolean stream(MapStore.Entry entry) throws IOException {
                        K k = keyValueMarshaller.bytesKey(entry.key, 0);
                        V v = keyValueMarshaller.bytesValue(k, entry.payload, 0);
                        return stream.stream(k, v);
                    }
                });
            }
        });
    }

    @Override
    public boolean streamKeys(final KeyStream<K> stream) throws IOException {
        return namedMap.stream(name, new TxStream<byte[], MapContext, ChunkFiler>() {

            @Override
            public boolean stream(byte[] key, MapContext monkey, ChunkFiler filer, Object lock) throws IOException {
                if (monkey == null || filer == null) {
                    return true;
                }
                return MapStore.INSTANCE.streamKeys(filer, monkey, lock, new MapStore.KeyStream() {

                    @Override
                    public boolean stream(byte[] key) throws IOException {
                        K k = keyValueMarshaller.bytesKey(key, 0);
                        return stream.stream(k);
                    }
                });
            }
        });
    }
}
