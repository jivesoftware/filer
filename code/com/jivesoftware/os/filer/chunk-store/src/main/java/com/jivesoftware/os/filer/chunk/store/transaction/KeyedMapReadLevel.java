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
import com.jivesoftware.os.filer.io.LocksProvider;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.map.store.MapContext;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 * @param <K> map's name
 * @param <H> hint type for creation
 * @param <M> Monkey type
 */
public class KeyedMapReadLevel<K, H, M extends MapContext> implements LevelProvider<ChunkStore, KeyedFPIndex<K>, ChunkStore, K, MonkeyAndFiler<M>> {

    private final OpenFiler<M, ChunkFiler> opener;
    private final LocksProvider<K> locksProvider;

    public KeyedMapReadLevel(OpenFiler<M, ChunkFiler> opener, LocksProvider<K> locksProvider) {
        this.opener = opener;
        this.locksProvider = locksProvider;
    }

    @Override
    public <R> R enter(ChunkStore chunkStore,
        KeyedFPIndex<K> parentStore,
        K mapName,
        StoreTransaction<R, ChunkStore, MonkeyAndFiler<M>> storeTransaction) throws IOException {
        return MapTransactionUtil.INSTANCE.read(chunkStore, parentStore, locksProvider, mapName, opener, chunkStore, storeTransaction);
    }

}
