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
import com.jivesoftware.os.filer.chunk.store.transaction.KeyedFilerRewriteLevel.RewriteFiler;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.LocksProvider;
import com.jivesoftware.os.filer.io.OpenFiler;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 * @param <M> monkey type
 * @param <K> filer's name
 */
public class KeyedFilerRewriteLevel<M, K> implements LevelProvider<ChunkStore, KeyedFPIndex<K>, ChunkStore, K, RewriteFiler<M>> {

    private final LocksProvider<K> locksProvider;
    private final long filerSize;
    private final CreateFiler<Long, M, ChunkFiler> creator;
    private final OpenFiler<M, ChunkFiler> opener;

    public KeyedFilerRewriteLevel(LocksProvider<K> locksProvider,
        long filerSize,
        CreateFiler<Long, M, ChunkFiler> creator,
        OpenFiler<M, ChunkFiler> opener) {
        this.locksProvider = locksProvider;
        this.filerSize = filerSize;
        this.creator = creator;
        this.opener = opener;
    }

    @Override
    public <R> R enter(final ChunkStore chunkStore,
        final KeyedFPIndex<K> parentStore,
        final K levelKey,
        final StoreTransaction<R, ChunkStore, RewriteFiler<M>> storeTransaction) throws IOException {

        final AtomicReference<R> result = new AtomicReference<>();
        return parentStore.commit(chunkStore, levelKey, filerSize, creator, opener, new GrowFiler<Long, M, ChunkFiler>() {

            @Override
            public Long grow(M monkey, ChunkFiler filer) throws IOException {
                return filerSize;
            }

            @Override
            public void grow(M currentMonkey, ChunkFiler currentFiler, M newMonkey, ChunkFiler newFiler) throws IOException {
                result.set(storeTransaction.commit(chunkStore, new RewriteFiler<>(currentMonkey, currentFiler, newMonkey, newFiler)));
            }
        }, new ChunkTransaction<M, R>() {

            @Override
            public R commit(M monkey, ChunkFiler filer) throws IOException {
                return result.get();
            }
        });

    }

    public static class RewriteFiler<M> {

        public final M oldMonkey;
        public final ChunkFiler oldFiler;
        public M newMonkey;
        public final ChunkFiler newFiler;

        /**
         *
         * @param oldMonkey nullable
         * @param oldFiler nullable
         * @param newMonkey
         * @param newFiler
         */
        public RewriteFiler(M oldMonkey, ChunkFiler oldFiler, M newMonkey, ChunkFiler newFiler) {
            this.oldMonkey = oldMonkey;
            this.oldFiler = oldFiler;
            this.newMonkey = newMonkey;
            this.newFiler = newFiler;
        }

    }

}
