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

import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMap;
import com.jivesoftware.os.filer.chunk.store.transaction.TxStream;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.KeyMarshaller;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyValueContext;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.api.KeyValueTransaction;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import java.io.IOException;
import java.util.List;

/**
 * @param <K>
 * @param <V>
 * @author jonathan.colt
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
            public Integer acquire(MapContext monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    if (MapStore.INSTANCE.acquire(monkey, 1)) {
                        return null;
                    } else {
                        return MapStore.INSTANCE.nextGrowSize(monkey, 1);
                    }
                }
            }

            @Override
            public void growAndAcquire(MapContext currentMonkey,
                ChunkFiler currentFiler,
                MapContext newMonkey,
                ChunkFiler newFiler,
                Object currentLock,
                Object newLock) throws IOException {

                synchronized (currentLock) {
                    synchronized (newLock) {
                        final Object[] newValues = new Object[newMonkey.capacity];
                        MapStore.INSTANCE.copyTo(currentFiler, currentMonkey, newFiler, newMonkey, new MapStore.CopyToStream() {
                            @Override
                            public void copied(long fromIndex, long toIndex) {
                                newValues[(int) toIndex] = values[(int) fromIndex];
                            }
                        });
                        values = newValues;

                    }
                }
            }

            @Override
            public void release(MapContext monkey, Object lock) {
                synchronized (lock) {
                    MapStore.INSTANCE.release(monkey, 1);
                }
            }
        };

        this.namedMap = new TxNamedMap(chunkStore, SKY_HOOK_FP, creator, opener, grower);
    }

    @Override
    public boolean[] contains(List<K> keys) throws IOException {
        final byte[][] keysBytes = new byte[keys.size()][];
        for (int i = 0; i < keysBytes.length; i++) {
            K key = keys.get(i);
            keysBytes[i] = key != null ? keyMarshaller.keyBytes(key) : null;
        }
        return namedMap.read(mapName, new ChunkTransaction<MapContext, boolean[]>() {
            @Override
            public boolean[] commit(MapContext context, ChunkFiler filer, Object lock) throws IOException {
                boolean[] result = new boolean[keysBytes.length];
                if (filer != null) {
                    synchronized (lock) {
                        for (int i = 0; i < keysBytes.length; i++) {
                            result[i] = (keysBytes[i] != null && MapStore.INSTANCE.contains(filer, context, keysBytes[i]));
                        }
                    }
                }
                return result;
            }
        });
    }

    @Override
    public <R> R execute(K key,
        boolean createIfAbsent,
        final KeyValueTransaction<V, R> keyValueTransaction) throws IOException {

        final byte[] keyBytes = keyMarshaller.keyBytes(key);
        if (createIfAbsent) {
            return namedMap.readWriteAutoGrow(mapName, new ChunkTransaction<MapContext, R>() {

                @Override
                public R commit(final MapContext monkey, final ChunkFiler filer, final Object lock) throws IOException {
                    return keyValueTransaction.commit(new KeyValueContext<V>() {

                        @Override
                        public void set(V value) throws IOException {
                            synchronized (lock) {
                                long ai = MapStore.INSTANCE.add(filer, monkey, (byte) 1, keyBytes, EMPTY_PAYLOAD);
                                values[(int) ai] = value;
                            }
                        }

                        @Override
                        public void remove() throws IOException {
                            synchronized (lock) {
                                long ai = MapStore.INSTANCE.remove(filer, monkey, keyBytes);
                                values[(int) ai] = null;
                            }
                        }

                        @Override
                        @SuppressWarnings("unchecked")
                        public V get() throws IOException {
                            synchronized (lock) {
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
                public R commit(final MapContext monkey, final ChunkFiler filer, final Object lock) throws IOException {
                    return keyValueTransaction.commit(new KeyValueContext<V>() {

                        @Override
                        public void set(V value) throws IOException {
                            throw new IllegalStateException("cannot set within a read tx.");
                        }

                        @Override
                        public void remove() throws IOException {
                            if (monkey != null && filer != null) {
                                synchronized (lock) {
                                    long ai = MapStore.INSTANCE.remove(filer, monkey, keyBytes);
                                    if (ai > -1) {
                                        values[(int) ai] = null;
                                    }
                                }
                            }
                        }

                        @Override
                        @SuppressWarnings("unchecked")
                        public V get() throws IOException {
                            if (monkey != null && filer != null) {
                                synchronized (lock) {
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
            public boolean stream(byte[] key, MapContext monkey, ChunkFiler filer, Object lock) throws IOException {
                return MapStore.INSTANCE.stream(filer, monkey, lock, new MapStore.EntryStream() {

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
            public boolean stream(byte[] key, MapContext monkey, ChunkFiler filer, Object lock) throws IOException {
                if (monkey == null || filer == null) {
                    return true;
                }
                return MapStore.INSTANCE.streamKeys(filer, monkey, lock, new MapStore.KeyStream() {

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
