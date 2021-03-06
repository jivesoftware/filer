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

import com.jivesoftware.os.filer.chunk.store.transaction.IntIndexSemaphore;
import com.jivesoftware.os.filer.chunk.store.transaction.MapBackedKeyedFPIndex;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCog;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMap;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.KeyMarshaller;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.IndexAlignedKeyValueTransaction;
import com.jivesoftware.os.filer.io.api.KeyValueContext;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.api.KeyValueTransaction;
import com.jivesoftware.os.filer.io.api.StackBuffer;
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

    static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final KeyMarshaller<K> keyMarshaller;
    private final byte[] mapName;
    private final TxNamedMap namedMap;
    private Object[] values;

    public TxKeyObjectStore(TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyHookCog,
        IntIndexSemaphore keySemaphores,
        int seed,
        ChunkStore chunkStore,
        KeyMarshaller<K> keyMarshaller,
        byte[] name,
        final int initialCapacity,
        final int keySize,
        final boolean variableKeySize) {
        this.keyMarshaller = keyMarshaller;
        this.mapName = name;

        OpenFiler<MapContext, ChunkFiler> opener = (filer, stackBuffer) -> {
            MapContext mapContext = MapStore.INSTANCE.open(filer, stackBuffer);
            if (values == null) {
                values = new Object[mapContext.capacity];
            }
            return mapContext;
        };

        CreateFiler<Integer, MapContext, ChunkFiler> creator = new CreateFiler<Integer, MapContext, ChunkFiler>() {
            @Override
            public MapContext create(Integer hint, ChunkFiler filer, StackBuffer stackBuffer) throws IOException {
                hint += initialCapacity;
                hint = hint < 2 ? 2 : hint;
                MapContext mapContext = MapStore.INSTANCE.create(hint, keySize, variableKeySize, 0, false, filer, stackBuffer);
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
            public Integer acquire(Integer sizeHint, MapContext monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    if (MapStore.INSTANCE.acquire(monkey, sizeHint)) {
                        return null;
                    } else {
                        return MapStore.INSTANCE.nextGrowSize(monkey, sizeHint);
                    }
                }
            }

            @Override
            public void growAndAcquire(Integer sizeHint,
                MapContext currentMonkey,
                ChunkFiler currentFiler,
                MapContext newMonkey,
                ChunkFiler newFiler,
                Object currentLock,
                Object newLock,
                StackBuffer stackBuffer) throws IOException {

                synchronized (currentLock) {
                    synchronized (newLock) {
                        final Object[] newValues = new Object[newMonkey.capacity];
                        MapStore.INSTANCE.copyTo(currentFiler, currentMonkey, newFiler, newMonkey,
                            (fromIndex, toIndex) -> newValues[(int) toIndex] = values[(int) fromIndex], stackBuffer);
                        values = newValues;

                    }
                }
            }

            @Override
            public void release(Integer sizeHint, MapContext monkey, Object lock) {
                synchronized (lock) {
                    MapStore.INSTANCE.release(monkey, sizeHint);
                }
            }
        };

        this.namedMap = new TxNamedMap(skyHookCog, seed, chunkStore, creator, opener, grower, keySemaphores);
    }

    @Override
    public boolean[] contains(List<K> keys, StackBuffer stackBuffer) throws IOException, InterruptedException {
        final byte[][] keysBytes = new byte[keys.size()][];
        for (int i = 0; i < keysBytes.length; i++) {
            K key = keys.get(i);
            keysBytes[i] = key != null ? keyMarshaller.keyBytes(key) : null;
        }
        return namedMap.read(mapName, (context, filer, _stackBuffer, lock) -> {
            boolean[] result = new boolean[keysBytes.length];
            if (filer != null) {
                synchronized (lock) {
                    for (int i = 0; i < keysBytes.length; i++) {
                        result[i] = (keysBytes[i] != null && MapStore.INSTANCE.contains(filer, context, keysBytes[i], _stackBuffer));
                    }
                }
            }
            return result;
        }, stackBuffer);
    }

    @Override
    public void multiExecute(K[] keys, IndexAlignedKeyValueTransaction<V> indexAlignedKeyValueTransaction, StackBuffer stackBuffer) throws IOException {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public <R> R execute(K key,
        boolean createIfAbsent,
        final KeyValueTransaction<V, R> keyValueTransaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {

        final byte[] keyBytes = keyMarshaller.keyBytes(key);
        if (createIfAbsent) {
            return namedMap.readWriteAutoGrow(mapName, 1,
                (monkey, filer, _stackBuffer, lock) -> keyValueTransaction.commit(new KeyValueContext<V>() {

                    @Override
                    public void set(V value) throws IOException {
                        synchronized (lock) {
                            long ai = MapStore.INSTANCE.add(filer, monkey, (byte) 1, keyBytes, EMPTY_PAYLOAD, _stackBuffer);
                            values[(int) ai] = value;
                        }
                    }

                    @Override
                    public void remove() throws IOException {
                        synchronized (lock) {
                            long ai = MapStore.INSTANCE.remove(filer, monkey, keyBytes, _stackBuffer);
                            values[(int) ai] = null;
                        }
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public V get() throws IOException {
                        synchronized (lock) {
                            long ai = MapStore.INSTANCE.get(filer, monkey, keyBytes, _stackBuffer);
                            if (ai > -1) {
                                return (V) values[(int) ai];
                            }
                            return null;
                        }
                    }
                }), stackBuffer);
        } else {
            return namedMap.read(mapName,
                (monkey, filer, _stackBuffer, lock) -> keyValueTransaction.commit(new KeyValueContext<V>() {

                    @Override
                    public void set(V value) throws IOException {
                        throw new IllegalStateException("cannot set within a read tx.");
                    }

                    @Override
                    public void remove() throws IOException {
                        if (monkey != null && filer != null) {
                            synchronized (lock) {
                                long ai = MapStore.INSTANCE.remove(filer, monkey, keyBytes, _stackBuffer);
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
                                long ai = MapStore.INSTANCE.get(filer, monkey, keyBytes, _stackBuffer);
                                if (ai > -1) {
                                    return (V) values[(int) ai];
                                }
                            }
                        }
                        return null;
                    }
                }), stackBuffer);
        }
    }

    @Override
    public boolean stream(final EntryStream<K, V> stream, StackBuffer stackBuffer) throws IOException, InterruptedException {

        return namedMap.stream(mapName,
            (key, monkey, filer, lock) -> MapStore.INSTANCE.stream(filer, monkey, lock, entry -> {
                K key1 = keyMarshaller.bytesKey(entry.key, 0);
                return stream.stream(key1, (V) values[entry.payloadIndex]);
            }, stackBuffer), stackBuffer);

    }

    @Override
    public boolean streamKeys(final KeyStream<K> stream, StackBuffer stackBuffer) throws IOException, InterruptedException {
        return namedMap.stream(mapName,
            (key, monkey, filer, lock) -> {
                if (monkey == null || filer == null) {
                    return true;
                }
                return MapStore.INSTANCE.streamKeys(filer, monkey, lock, key1 -> {
                    K k = keyMarshaller.bytesKey(key1, 0);
                    return stream.stream(k);
                }, stackBuffer);
            }, stackBuffer);
    }
}
