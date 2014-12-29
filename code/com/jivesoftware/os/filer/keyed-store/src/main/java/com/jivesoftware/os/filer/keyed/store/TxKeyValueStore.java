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
import com.jivesoftware.os.filer.chunk.store.transaction.TxStream;
import com.jivesoftware.os.filer.io.ByteArrayPartitionFunction;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 * @param <K>
 * @param <V>
 */
public class TxKeyValueStore<K, V> implements KeyValueStore<K, V> {

    static final long _skyHookFP = 464; // I died a little bit doing this.
    private final KeyValueMarshaller<K, V> keyValueMarshaller;
    private final byte[] name;
    private final TxNamedMap namedMap;

    public TxKeyValueStore(ChunkStore[] chunkStores, KeyValueMarshaller<K, V> keyValueMarshaller, byte[] name, int keySize, boolean variableKeySize,
        int payloadSize, boolean variablePayloadSize) {
        this.keyValueMarshaller = keyValueMarshaller;
        this.name = name;
        this.namedMap = new TxNamedMap(chunkStores, _skyHookFP,
            new ByteArrayPartitionFunction(),
            new MapCreator(2, keySize, variableKeySize, payloadSize, variablePayloadSize),
            new MapOpener(),
            new MapGrower<>(1));
    }

    @Override
    public <R> R execute(final K key, boolean createIfAbsent, final KeyValueTransaction<V, R> keyValueTransaction) throws IOException {
        final byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        if (createIfAbsent) {
            return namedMap.write(keyBytes, name, new ChunkTransaction<MapContext, R>() {

                @Override
                public R commit(final MapContext monkey, final ChunkFiler filer) throws IOException {
                    return keyValueTransaction.commit(new KeyValueContext<V>() {

                        @Override
                        public void set(V value) throws IOException {
                            synchronized (monkey) {
                                MapStore.INSTANCE.add(filer, monkey, (byte) 1, keyBytes, keyValueMarshaller.valueBytes(value));
                            }
                        }

                        @Override
                        public void remove() throws IOException {
                            synchronized (monkey) {
                                MapStore.INSTANCE.remove(filer, monkey, keyBytes);
                            }
                        }

                        @Override
                        public V get() throws IOException {
                            throw new IllegalStateException("cannot get within a write tx.");
                        }
                    });
                }
            });
        } else {
            return namedMap.read(keyBytes, name, new ChunkTransaction<MapContext, R>() {

                @Override
                public R commit(final MapContext monkey, final ChunkFiler filer) throws IOException {
                    return keyValueTransaction.commit(new KeyValueContext<V>() {

                        @Override
                        public void set(V value) throws IOException {
                            throw new IllegalStateException("cannot set within a read tx.");
                        }

                        @Override
                        public void remove() throws IOException {
                            throw new IllegalStateException("cannot remove within a read tx.");
                        }

                        @Override
                        public V get() throws IOException {
                            synchronized (monkey) {
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
        }
    }

    @Override
    public boolean stream(final EntryStream<K, V> stream) throws IOException {
        return namedMap.stream(name, new TxStream<byte[], MapContext, ChunkFiler>() {

            @Override
            public boolean stream(byte[] key, MapContext monkey, ChunkFiler filer) throws IOException {
                return MapStore.INSTANCE.stream(filer, monkey, new MapStore.EntryStream() {

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
            public boolean stream(byte[] key, MapContext monkey, ChunkFiler filer) throws IOException {
                return MapStore.INSTANCE.streamKeys(filer, monkey, new MapStore.KeyStream() {

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
