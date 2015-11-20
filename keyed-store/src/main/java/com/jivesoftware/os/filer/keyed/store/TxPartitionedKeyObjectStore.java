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

import com.jivesoftware.os.filer.io.PartitionFunction;
import com.jivesoftware.os.filer.io.api.IndexAlignedKeyValueTransaction;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.api.KeyValueTransaction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 * @param <K>
 * @param <V>
 */
public class TxPartitionedKeyObjectStore<K, V> implements KeyValueStore<K, V> {

    private final PartitionFunction<K> partitionFunction;
    private final TxKeyObjectStore<K, V>[] stores;

    public TxPartitionedKeyObjectStore(PartitionFunction<K> partitionFunction,
        TxKeyObjectStore<K, V>[] stores) {
        this.partitionFunction = partitionFunction;
        this.stores = stores;
    }

    @Override
    public boolean[] contains(List<K> keys, byte[] primitiveBuffer) throws IOException {
        List[] partitionedKeys = new List[stores.length];
        for (int p = 0; p < stores.length; p++) {
            partitionedKeys[p] = new ArrayList();
            for (int i = 0; i < keys.size(); i++) {
                partitionedKeys[p].add(null);
            }
        }
        for (int i = 0; i < keys.size(); i++) {
            K key = keys.get(i);
            int p = partitionFunction.partition(stores.length, key);
            partitionedKeys[p].set(i, key);
        }
        boolean[][] partitionedContains = new boolean[stores.length][];
        for (int p = 0; p < stores.length; p++) {
            partitionedContains[p] = stores[p].contains((List<K>) partitionedKeys[p], primitiveBuffer);
        }

        boolean[] result = new boolean[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            for (int p = 0; p < stores.length; p++) {
                result[i] |= partitionedContains[p][i];
            }
        }
        return result;
    }

    @Override
    public void multiExecute(K[] keys, IndexAlignedKeyValueTransaction<V> indexAlignedKeyValueTransaction, byte[] primitiveBuffer) throws IOException {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public <R> R execute(K key,
        boolean createIfAbsent,
        KeyValueTransaction<V, R> keyValueTransaction,
        byte[] primitiveBuffer) throws IOException {
        return stores[partitionFunction.partition(stores.length, key)].execute(key, createIfAbsent, keyValueTransaction, primitiveBuffer);
    }

    @Override
    public boolean stream(final EntryStream<K, V> stream, byte[] primitiveBuffer) throws IOException {
        for (TxKeyObjectStore<K, V> store : stores) {
            if (!store.stream(stream, primitiveBuffer)) {
                return false;
            }
        }
        return true;

    }

    @Override
    public boolean streamKeys(final KeyStream<K> stream, byte[] primitiveBuffer) throws IOException {
        for (TxKeyObjectStore<K, V> store : stores) {
            if (!store.streamKeys(stream, primitiveBuffer)) {
                return false;
            }
        }
        return true;
    }
}
