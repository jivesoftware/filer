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
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMap;
import com.jivesoftware.os.filer.chunk.store.transaction.TxStream;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.KeyMarshaller;
import com.jivesoftware.os.filer.io.OpenFiler;
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
public class TxKeyObjectStore<K, V> implements KeyValueStore<K, V> {

    static final long SKY_HOOK_FP = 464; // I died a little bit doing this.
    static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final KeyMarshaller<K> keyMarshaller;
    private final byte[] mapName;
    private final TxNamedMap namedMap;
    private Object[] values;

    public TxKeyObjectStore(ChunkStore chunkStore,
        KeyMarshaller<K> keyMarshaller,
        byte[] name,
        final int initialCapacity,
        final int keySize,
        final boolean variableKeySize) {
        this.keyMarshaller = keyMarshaller;
        this.mapName = name;

        OpenFiler<MapContext, ChunkFiler> opener = new OpenFiler<MapContext, ChunkFiler>() {

            @Override
            public MapContext open(ChunkFiler filer) throws IOException {
                MapContext mapContext = MapStore.INSTANCE.open(filer);
                values = new Object[mapContext.capacity];
                return mapContext;
            }
        };

        CreateFiler<Integer, MapContext, ChunkFiler> creator = new CreateFiler<Integer, MapContext, ChunkFiler>() {
            @Override
            public MapContext create(Integer hint, ChunkFiler filer) throws IOException {
                hint += initialCapacity;
                hint = hint < 2 ? 2 : hint;
                MapContext mapContext = MapStore.INSTANCE.create(hint, keySize, variableKeySize, 0, false, filer);
                values = new Object[mapContext.capacity];
                return mapContext;
            }

            @Override
            public long sizeInBytes(Integer hint) throws IOException {
                hint += initialCapacity;
                hint = hint < 2 ? 2 : hint;
                return MapStore.INSTANCE.computeFilerSize(hint, keySize, variableKeySize, 0, false);
            }
        };

        GrowFiler<Integer, MapContext, ChunkFiler> grower = new GrowFiler<Integer, MapContext, ChunkFiler>() {
            @Override
            public Integer grow(MapContext monkey, ChunkFiler filer) throws IOException {
                synchronized (monkey) {
                    if (MapStore.INSTANCE.isFullWithNMore(filer, monkey, 1)) {
                        return MapStore.INSTANCE.nextGrowSize(monkey, 1);
                    }
                }
                return null;
            }

            @Override
            public void grow(MapContext currentMonkey, ChunkFiler currentFiler, MapContext newMonkey, ChunkFiler newFiler) throws IOException {
                synchronized (currentMonkey) {
                    synchronized (newMonkey) {
                        final Object[] newValues = new Object[newMonkey.capacity];
                        MapStore.INSTANCE.copyTo(currentFiler, currentMonkey, newFiler, newMonkey, new MapStore.CopyToStream() {
                            @Override
                            public void copied(int fromIndex, int toIndex) {
                                newValues[toIndex] = values[fromIndex];
                            }
                        });
                        values = newValues;

                    }
                }
            }
        };

        this.namedMap = new TxNamedMap(chunkStore, SKY_HOOK_FP, creator, opener, grower);
    }

    @Override
    public <R> R execute(K key,
        boolean createIfAbsent,
        final KeyValueTransaction<V, R> keyValueTransaction) throws IOException {

        final byte[] keyBytes = keyMarshaller.keyBytes(key);
        if (createIfAbsent) {
            return namedMap.write(mapName, new ChunkTransaction<MapContext, R>() {

                @Override
                public R commit(final MapContext monkey, final ChunkFiler filer) throws IOException {
                    return keyValueTransaction.commit(new KeyValueContext<V>() {

                        @Override
                        public void set(V value) throws IOException {
                            synchronized (monkey) {
                                int ai = MapStore.INSTANCE.add(filer, monkey, (byte) 1, keyBytes, EMPTY_PAYLOAD);
                                values[ai] = value;
                            }
                        }

                        @Override
                        public void remove() throws IOException {
                            synchronized (monkey) {
                                int ai = MapStore.INSTANCE.remove(filer, monkey, keyBytes);
                                values[ai] = null;
                            }
                        }

                        @Override
                        @SuppressWarnings("unchecked")
                        public V get() throws IOException {
                            synchronized (monkey) {
                                long ai = MapStore.INSTANCE.get(filer, monkey, keyBytes);
                                if (ai > -1) {
                                    return (V) values[(int) ai];
                                }
                                return null;
                            }
                        }
                    });
                }
            });
        } else {
            return namedMap.read(mapName, new ChunkTransaction<MapContext, R>() {

                @Override
                public R commit(final MapContext monkey, final ChunkFiler filer) throws IOException {
                    return keyValueTransaction.commit(new KeyValueContext<V>() {

                        @Override
                        public void set(V value) throws IOException {
                            throw new IllegalStateException("cannot set within a read tx.");
                        }

                        @Override
                        public void remove() throws IOException {
                            if (monkey != null && filer != null) {
                                synchronized (monkey) {
                                    int ai = MapStore.INSTANCE.remove(filer, monkey, keyBytes);
                                    if (ai > -1) {
                                        values[ai] = null;
                                    }
                                }
                            }
                        }

                        @Override
                        @SuppressWarnings("unchecked")
                        public V get() throws IOException {
                            if (monkey != null && filer != null) {
                                synchronized (monkey) {
                                    long ai = MapStore.INSTANCE.get(filer, monkey, keyBytes);
                                    if (ai > -1) {
                                        return (V) values[(int) ai];
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

        return namedMap.stream(mapName, new TxStream<byte[], MapContext, ChunkFiler>() {

            @Override
            public boolean stream(byte[] key, MapContext monkey, ChunkFiler filer) throws IOException {
                return MapStore.INSTANCE.stream(filer, monkey, new MapStore.EntryStream() {

                    @Override
                    public boolean stream(MapStore.Entry entry) throws IOException {
                        K key = keyMarshaller.bytesKey(entry.key, 0);
                        return stream.stream(key, (V) values[entry.payloadIndex]);
                    }
                });
            }
        });

    }

    @Override
    public boolean streamKeys(final KeyStream<K> stream) throws IOException {
        return namedMap.stream(mapName, new TxStream<byte[], MapContext, ChunkFiler>() {

            @Override
            public boolean stream(byte[] key, MapContext monkey, ChunkFiler filer) throws IOException {
                return MapStore.INSTANCE.streamKeys(filer, monkey, new MapStore.KeyStream() {

                    @Override
                    public boolean stream(byte[] key) throws IOException {
                        K k = keyMarshaller.bytesKey(key, 0);
                        return stream.stream(k);
                    }
                });
            }
        });
    }
}
