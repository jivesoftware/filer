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

import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.LocksProvider;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import java.io.IOException;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class MapBackedKeyedFPIndexOpener implements OpenFiler<MapBackedKeyedFPIndex, ChunkFiler> {

    private final int seed;
    private final LocksProvider<byte[]> keyLocks;
    private final SemaphoreProvider<byte[]> keySemaphores;
    private final KeyToFPCacheFactory cacheFactory; // Nullable

    public MapBackedKeyedFPIndexOpener(int seed, LocksProvider<byte[]> keyLocks, SemaphoreProvider<byte[]> keySemaphores, KeyToFPCacheFactory cacheFactory) {
        this.seed = seed;
        this.keyLocks = keyLocks;
        this.keySemaphores = keySemaphores;
        this.cacheFactory = cacheFactory;
    }

    @Override
    public MapBackedKeyedFPIndex open(ChunkFiler filer) throws IOException {
        MapContext mapContext = MapStore.INSTANCE.open(filer);
        Map<IBA, Long> keyFPCache = null;
        if (cacheFactory != null) {
            keyFPCache = cacheFactory.createCache();
        }
        return new MapBackedKeyedFPIndex(seed, filer.getChunkStore(), filer.getChunkFP(), mapContext, this, keyLocks, keySemaphores, keyFPCache);
    }

}
