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
import com.jivesoftware.os.filer.chunk.store.transaction.MapCreator;
import com.jivesoftware.os.filer.chunk.store.transaction.MapGrower;
import com.jivesoftware.os.filer.chunk.store.transaction.MapOpener;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCog;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMap;
import com.jivesoftware.os.filer.chunk.store.transaction.TxPartitionedNamedMap;
import com.jivesoftware.os.filer.io.ByteArrayPartitionFunction;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.io.api.IndexAlignedKeyValueTransaction;
import com.jivesoftware.os.filer.io.api.KeyValueContext;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.api.KeyValueTransaction;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.map.MapStore;
import java.io.IOException;
import java.util.List;

/**
 * @param <K>
 * @param <V>
 * @author jonathan.colt
 */
public class TxKeyValueStore<K, V> implements KeyValueStore<K, V> {

    private final KeyValueMarshaller<K, V> keyValueMarshaller;
    private final byte[] name;
    private final TxPartitionedNamedMap namedMap;

    public TxKeyValueStore(TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyhookCog,
        IntIndexSemaphore skyHookKeySemaphores,
        int seed,
        ChunkStore[] chunkStores,
        KeyValueMarshaller<K, V> keyValueMarshaller,
        byte[] name,
        int keySize,
        boolean variableKeySize,
        int payloadSize,
        boolean variablePayloadSize) {
        this.keyValueMarshaller = keyValueMarshaller;
        this.name = name;

        // TODO consider replacing with builder pattern
        TxNamedMap[] stores = new TxNamedMap[chunkStores.length];
        for (int i = 0; i < stores.length; i++) {
            stores[i] = new TxNamedMap(skyhookCog, seed, chunkStores[i],
                new MapCreator(2, keySize, variableKeySize, payloadSize, variablePayloadSize),
                MapOpener.INSTANCE,
                new MapGrower<>(),
                skyHookKeySemaphores);
        }

        this.namedMap = new TxPartitionedNamedMap(ByteArrayPartitionFunction.INSTANCE, stores);
    }

    @Override
    public boolean[] contains(List<K> keys, StackBuffer stackBuffer) throws IOException, InterruptedException {
        byte[][] keysBytes = new byte[keys.size()][];
        for (int i = 0; i < keysBytes.length; i++) {
            keysBytes[i] = keyValueMarshaller.keyBytes(keys.get(i));
        }
        return namedMap.contains(keysBytes, name, stackBuffer);
    }

    @Override
    public void multiExecute(K[] keys, IndexAlignedKeyValueTransaction<V> indexAlignedKeyValueTransaction, StackBuffer stackBuffer) throws IOException,
        InterruptedException {
        byte[][] keysBytes = new byte[keys.length][];
        for (int i = 0; i < keysBytes.length; i++) {
            keysBytes[i] = keys[i] != null ? keyValueMarshaller.keyBytes(keys[i]) : null;
        }
        namedMap.multiReadWriteAutoGrow(keysBytes, name,
            (monkey, filer, stackBuffer1, lock, index) -> {
                synchronized (lock) {
                    indexAlignedKeyValueTransaction.commit(new KeyValueContext<V>() {

                        @Override
                        public void set(V value) throws IOException {
                            MapStore.INSTANCE.add(filer, monkey, (byte) 1, keysBytes[index], keyValueMarshaller.valueBytes(value), stackBuffer1);
                        }

                        @Override
                        public void remove() throws IOException {
                            MapStore.INSTANCE.remove(filer, monkey, keysBytes[index], stackBuffer1);
                        }

                        @Override
                        public V get() throws IOException {
                            long pi = MapStore.INSTANCE.get(filer, monkey, keysBytes[index], stackBuffer1);
                            if (pi > -1) {
                                byte[] rawValue = MapStore.INSTANCE.getPayload(filer, monkey, pi, stackBuffer1);
                                return keyValueMarshaller.bytesValue(keys[index], rawValue, 0);
                            }
                            return null;
                        }
                    }, index);
                }
                return null;
            }, stackBuffer);
    }

    @Override
    public <R> R execute(final K key, boolean createIfAbsent, final KeyValueTransaction<V, R> keyValueTransaction, StackBuffer stackBuffer) throws IOException,
        InterruptedException {
        final byte[] keyBytes = keyValueMarshaller.keyBytes(key);
        if (createIfAbsent) {
            return namedMap.readWriteAutoGrow(keyBytes, name,
                (monkey, filer, _stackBuffer, lock) -> keyValueTransaction.commit(new KeyValueContext<V>() {

                    @Override
                    public void set(V value) throws IOException {
                        synchronized (lock) {
                            MapStore.INSTANCE.add(filer, monkey, (byte) 1, keyBytes, keyValueMarshaller.valueBytes(value), _stackBuffer);
                        }
                    }

                    @Override
                    public void remove() throws IOException {
                        synchronized (lock) {
                            MapStore.INSTANCE.remove(filer, monkey, keyBytes, _stackBuffer);
                        }
                    }

                    @Override
                    public V get() throws IOException {
                        synchronized (lock) {
                            long pi = MapStore.INSTANCE.get(filer, monkey, keyBytes, _stackBuffer);
                            if (pi > -1) {
                                byte[] rawValue = MapStore.INSTANCE.getPayload(filer, monkey, pi, _stackBuffer);
                                return keyValueMarshaller.bytesValue(key, rawValue, 0);
                            }
                            return null;
                        }
                    }
                }), stackBuffer);
        } else {
            return namedMap.read(keyBytes, name,
                (monkey, filer, _stackBuffer, lock) -> keyValueTransaction.commit(new KeyValueContext<V>() {

                    @Override
                    public void set(V value) throws IOException {
                        throw new IllegalStateException("cannot set within a read tx.");
                    }

                    @Override
                    public void remove() throws IOException {
                        if (filer != null && monkey != null) {
                            synchronized (lock) {
                                MapStore.INSTANCE.remove(filer, monkey, keyBytes, _stackBuffer);
                            }
                        }
                    }

                    @Override
                    public V get() throws IOException {
                        if (filer != null && monkey != null) {
                            synchronized (lock) {
                                long pi = MapStore.INSTANCE.get(filer, monkey, keyBytes, _stackBuffer);
                                if (pi > -1) {
                                    byte[] rawValue = MapStore.INSTANCE.getPayload(filer, monkey, pi, _stackBuffer);
                                    return keyValueMarshaller.bytesValue(key, rawValue, 0);
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
        return namedMap.stream(name, (key, monkey, filer, lock) -> {
            if (monkey == null || filer == null) {
                return true;
            }
            return MapStore.INSTANCE.stream(filer, monkey, lock, entry -> {
                K k = keyValueMarshaller.bytesKey(entry.key, 0);
                V v = keyValueMarshaller.bytesValue(k, entry.payload, 0);
                return stream.stream(k, v);
            }, stackBuffer);
        }, stackBuffer);
    }

    @Override
    public boolean streamKeys(final KeyStream<K> stream, StackBuffer stackBuffer) throws IOException, InterruptedException {
        return namedMap.stream(name, (key, monkey, filer, lock) -> {
            if (monkey == null || filer == null) {
                return true;
            }
            return MapStore.INSTANCE.streamKeys(filer, monkey, lock, key1 -> {
                K k = keyValueMarshaller.bytesKey(key1, 0);
                return stream.stream(k);
            }, stackBuffer);
        }, stackBuffer);
    }
}
