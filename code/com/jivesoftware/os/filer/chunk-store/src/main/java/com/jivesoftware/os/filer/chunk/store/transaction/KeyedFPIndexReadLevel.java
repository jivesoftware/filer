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
import java.io.IOException;

/**
 *
 * Input: a chunk FP; Output: KeyedFPStore
 *
 * Given a chunkFP this class hands off a place to store filer pointers based on a power of 2 key.length() slots.
 *
 * @author jonathan.colt
 * @param <K>
 */
public class KeyedFPIndexReadLevel<K> implements LevelProvider<ChunkStore, KeyedFPIndex<K>, ChunkStore, K, PowerKeyedFPIndex> {

    @Override
    public <R> R enter(final ChunkStore chunkStore,
        KeyedFPIndex<K> parentStore,
        K key,
        final StoreTransaction<R, ChunkStore, PowerKeyedFPIndex> storeTransaction) throws IOException {

        return parentStore.commit(chunkStore, key, null, null, KeyedFPIndexOpener.DEFAULT, null, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                return storeTransaction.commit(chunkStore, monkey);
            }
        });

    }

}
