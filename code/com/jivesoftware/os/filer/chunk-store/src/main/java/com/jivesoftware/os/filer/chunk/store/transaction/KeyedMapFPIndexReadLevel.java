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

import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.io.LocksProvider;
import com.jivesoftware.os.filer.map.store.MapContext;
import java.io.IOException;

/**
 * Input: keySize example: key.length(); Output: KeyFPStore with a thats supports keys of provided keySize.
 *
 * @author jonathan.colt
 */
public class KeyedMapFPIndexReadLevel implements LevelProvider<ChunkStore, KeyedFPIndex<Integer>, ChunkStore, Integer, MapBackedKeyedFPIndex> {

    private final LocksProvider<Integer> locksProvider;

    public KeyedMapFPIndexReadLevel(LocksProvider<Integer> locksProvider) {
        this.locksProvider = locksProvider;
    }

    @Override
    public <R> R enter(ChunkStore chunkStore,
        KeyedFPIndex<Integer> parentStore,
        Integer keySize,
        final StoreTransaction<R, ChunkStore, MapBackedKeyedFPIndex> storeTransaction) throws IOException {

        return MapTransactionUtil.INSTANCE.read(chunkStore, parentStore, locksProvider, keySize, MapOpener.DEFAULT, chunkStore,
            new StoreTransaction<R, ChunkStore, MonkeyAndFiler<MapContext>>() {

                @Override
                public R commit(ChunkStore backingStorage, MonkeyAndFiler<MapContext> child) throws IOException {
                    return storeTransaction.commit(backingStorage, new MapBackedKeyedFPIndex(child.filer.getChunkStore(), child.filer.getChunkFP()));
                }
            });
    }

}
