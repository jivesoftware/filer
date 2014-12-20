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
import com.jivesoftware.os.filer.io.LocksProvider;
import com.jivesoftware.os.filer.io.OpenFiler;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 * @param <M> monkey type
 * @param <K> filer's name
 */
public class KeyedFilerReadOnlyLevel<M, K> implements LevelProvider<ChunkStore, KeyedFPIndex<K>, ChunkStore, K, MonkeyAndFiler<M>> {

    private final LocksProvider<K> locksProvider;
    private final OpenFiler<M, ChunkFiler> opener;

    public KeyedFilerReadOnlyLevel(LocksProvider<K> locksProvider,
        OpenFiler<M, ChunkFiler> opener) {
        this.locksProvider = locksProvider;
        this.opener = opener;
    }

    @Override
    public <R> R enter(final ChunkStore chunkStore,
        KeyedFPIndex<K> parentStore,
        final K levelKey,
        final StoreTransaction<R, ChunkStore, MonkeyAndFiler<M>> storeTransaction) throws IOException {

        return parentStore.commit(chunkStore, levelKey, null, null, opener, null, new ChunkTransaction<M, R>() {

            @Override
            public R commit(M monkey, ChunkFiler filer) throws IOException {
                Object lock = locksProvider.lock(levelKey);
                synchronized (lock) {
                    return storeTransaction.commit(chunkStore, new MonkeyAndFiler<>(monkey, filer));
                }
            }
        });
    }
}
