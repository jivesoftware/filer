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

import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class TransactionalKeyedFilerStore implements KeyedFilerStore {

    private final ChunkStore[] chunkStores;
    private final ByteArrayStripingLocksProvider locksProvider;
    private final byte[] name;

    public TransactionalKeyedFilerStore(ChunkStore[] chunkStores,
        ByteArrayStripingLocksProvider locksProvider,
        byte[] name) {
        this.chunkStores = chunkStores;
        this.locksProvider = locksProvider;
        this.name = name;
    }

    @Override
    public <R> R execute(byte[] keyBytes, long newFilerInitialCapacity, final FilerTransaction<Filer, R> transaction) throws IOException {

//        TransactionStore<ChunkStore[]> transactionStore = new TransactionStore<>(chunkStores);
//
//        List<LevelProvider> levelProviders = new ArrayList<>();
//        levelProviders.add(new ChunkPartitionerLevel());
//        levelProviders.add(new KeyedFPIndexReadLevel());
//        levelProviders.add(new KeyedMapFPIndexWriteLevel(true));
//        // WTF
//        levelProviders.add(new KeyedFPIndexReadLevel());
//        levelProviders.add(new KeyedMapFPIndexWriteLevel(true));
//
//        if (newFilerInitialCapacity > 0) {
//            levelProviders.add(new KeyedFilerRewriteLevel(locksProvider, newFilerInitialCapacity));
//        } else {
//            levelProviders.add(new KeyedFilerReadOnlyLevel(locksProvider));
//        }
//
//        List<Serializable> asList = Arrays.asList(keyBytes, 464, name.length, name, keyBytes.length, keyBytes);
//
//        return transactionStore.commit(asList, levelProviders, new StoreTransaction<R, ChunkStore, Filer>() {
//
//            @Override
//            public R commit(ChunkStore backingStorage, Filer child) throws IOException {
//                return transaction.commit(child);
//            }
//
//        });
        return null;
    }

    @Override
    public <R> R executeRewrite(byte[] keyBytes, long newFilerInitialCapacity, RewriteFilerTransaction<Filer, R> transaction) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean stream(KeyValueStore.EntryStream<IBA, Filer> stream) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean streamKeys(KeyValueStore.KeyStream<IBA> stream) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
