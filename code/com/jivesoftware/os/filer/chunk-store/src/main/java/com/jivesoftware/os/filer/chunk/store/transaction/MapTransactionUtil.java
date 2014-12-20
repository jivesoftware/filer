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
import com.jivesoftware.os.filer.map.store.MapStore;
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
        final StoreTransaction<R, S, MonkeyAndFiler<M>> storeTransaction) throws IOException {

        return parentStore.commit(chunkStore, key, null, null, opener, null, new ChunkTransaction<M, R>() {

            @Override
            public R commit(M monkey, ChunkFiler filer) throws IOException {
                return storeTransaction.commit(contextStorage, new MonkeyAndFiler<>(monkey, filer));
            }
        });

    }

    public <R, S, K, M extends MapContext> R write(final ChunkStore chunkStore,
        final CreateFiler<Integer, M, ChunkFiler> creator,
        final int alwaysRoomForNMoreKeys,
        final KeyedFPIndex<K> parentStore,
        final LocksProvider<K> locksProvider,
        final K key,
        final OpenFiler<M, ChunkFiler> opener,
        final S contextStorage,
        final StoreTransaction<R, S, MonkeyAndFiler<M>> storeTransaction) throws IOException {

        return parentStore.commit(chunkStore, key, alwaysRoomForNMoreKeys, creator, opener, new GrowFiler<Integer, M, ChunkFiler>() {

            @Override
            public Integer grow(M monkey, ChunkFiler filer) throws IOException {
                synchronized (monkey) {
                    if (MapStore.INSTANCE.isFullWithNMore(filer, monkey, alwaysRoomForNMoreKeys)) {
                        return MapStore.INSTANCE.nextGrowSize(monkey, alwaysRoomForNMoreKeys);
                    }
                }
                return null;
            }

            @Override
            public void grow(M currentMonkey, ChunkFiler currentFiler, M newMonkey, ChunkFiler newFiler) throws IOException {
                synchronized (currentMonkey) {
                    synchronized (newMonkey) {
                        MapStore.INSTANCE.copyTo(currentFiler, currentMonkey, newFiler, newMonkey, null);
                    }
                }
            }
        }, new ChunkTransaction<M, R>() {

            @Override
            public R commit(M monkey, ChunkFiler filer) throws IOException {
                return storeTransaction.commit(contextStorage, new MonkeyAndFiler<>(monkey, filer));
            }
        });

    }
}
