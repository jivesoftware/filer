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
import com.jivesoftware.os.filer.io.map.SkipListMapContext;
import com.jivesoftware.os.filer.io.map.SkipListMapStore;
import com.jivesoftware.os.filer.map.store.LexSkipListComparator;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class SkipListMapBackedKeyedFPIndexOpener implements OpenFiler<SkipListMapBackedKeyedFPIndex, ChunkFiler> {

    private final int seed;
    private final LocksProvider<byte[]> keyLocks;
    private final SemaphoreProvider<byte[]> keySemaphores;
    private final KeyToFPCacheFactory cacheFactory; // Nullable

    public SkipListMapBackedKeyedFPIndexOpener(int seed,
        LocksProvider<byte[]> keyLocks,
        SemaphoreProvider<byte[]> keySemaphores,
        KeyToFPCacheFactory cacheFactory) {
        this.seed = seed;
        this.keyLocks = keyLocks;
        this.keySemaphores = keySemaphores;
        this.cacheFactory = cacheFactory;
    }

    @Override
    public SkipListMapBackedKeyedFPIndex open(ChunkFiler filer, byte[] primitiveBuffer) throws IOException {
        MapContext mapContext = MapStore.INSTANCE.open(filer, primitiveBuffer);
        byte[] headKey = new byte[mapContext.keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext skipListMapContext = SkipListMapStore.INSTANCE.open(headKey, LexSkipListComparator.cSingleton, filer, primitiveBuffer);
        Map<IBA, Long> keyFPCache = null;
        if (cacheFactory != null) {
            keyFPCache = cacheFactory.createCache();
        }
        return new SkipListMapBackedKeyedFPIndex(seed,
            filer.getChunkStore(), filer.getChunkFP(), skipListMapContext, this, keyLocks, keySemaphores, keyFPCache);
    }

}
