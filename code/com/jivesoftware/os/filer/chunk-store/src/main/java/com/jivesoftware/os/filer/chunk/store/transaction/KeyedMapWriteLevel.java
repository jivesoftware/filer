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
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.LocksProvider;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.map.store.MapContext;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 * @param <K> map's name
 * @param <M> monkey type
 */
public class KeyedMapWriteLevel<K, M extends MapContext> implements LevelProvider<ChunkStore, KeyedFPIndex<K>, ChunkStore, K, MonkeyAndFiler<M>> {

    private final CreateFiler<Integer, M, ChunkFiler> creator;
    private final OpenFiler<M, ChunkFiler> opener;
    private final LocksProvider<K> locksProvider;
    private final int alwaysRoomForNMoreKeys;

    public KeyedMapWriteLevel(CreateFiler<Integer, M, ChunkFiler> creator,
        OpenFiler<M, ChunkFiler> opener,
        LocksProvider<K> locksProvider,
        int alwaysRoomForNMoreKeys) {
        this.creator = creator;
        this.opener = opener;
        this.locksProvider = locksProvider;
        this.alwaysRoomForNMoreKeys = alwaysRoomForNMoreKeys;
    }

    @Override
    public <R> R enter(ChunkStore chunkStore,
        KeyedFPIndex<K> parentStore,
        K key,
        StoreTransaction<R, ChunkStore, MonkeyAndFiler<M>> storeTransaction) throws IOException {

        return MapTransactionUtil.INSTANCE
            .write(chunkStore, creator, alwaysRoomForNMoreKeys, parentStore, locksProvider, key, opener, chunkStore, storeTransaction);

    }

}
