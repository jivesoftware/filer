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
package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkTransaction;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.LocksProvider;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.map.store.MapContext;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class MapTransactionUtil {

    public static final MapTransactionUtil INSTANCE = new MapTransactionUtil();

    private MapTransactionUtil() {
    }

    public <R, S, K, M extends MapContext> R read(final ChunkStore chunkStore,
        final KeyedFPIndex<K> parentStore,
        final LocksProvider<K> locksProvider,
        final K key,
        final OpenFiler<M, ChunkFiler> opener,
        final S contextStorage,
        final StoreTransaction<R, S, MonkeyAndKeyedFiler<M, K>> storeTransaction) throws IOException {

        return parentStore.commit(chunkStore, key, null, null, opener, null, new ChunkTransaction<M, R>() {

            @Override
            public R commit(M monkey, ChunkFiler filer) throws IOException {
                return storeTransaction.commit(contextStorage, new MonkeyAndKeyedFiler<>(monkey, key, filer));
            }
        });

    }

    public <R, S, K, H, M extends MapContext> R write(final ChunkStore chunkStore,
        final CreateFiler<H, M, ChunkFiler> creator,
        final H alwaysRoomForNMoreKeys,
        final KeyedFPIndex<K> parentStore,
        final LocksProvider<K> locksProvider,
        final K key,
        final OpenFiler<M, ChunkFiler> opener,
        final GrowFiler<H, M, ChunkFiler> grower,
        final S contextStorage,
        final StoreTransaction<R, S, MonkeyAndKeyedFiler<M, K>> storeTransaction) throws IOException {

        return parentStore.commit(chunkStore, key, alwaysRoomForNMoreKeys, creator, opener, grower, new ChunkTransaction<M, R>() {

            @Override
            public R commit(M monkey, ChunkFiler filer) throws IOException {
                return storeTransaction.commit(contextStorage, new MonkeyAndKeyedFiler<>(monkey, key, filer));
            }
        });

    }
}
