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

import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.map.MapContext;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author jonathan.colt
 */
public class TxNamedMap {

    private final ChunkStore chunkStore;
    private final long constantFP;

    private final TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyHookCog;
    private final CreateFiler<Void, PowerKeyedFPIndex, ChunkFiler> skyHookIndexCreator;
    private final OpenFiler<PowerKeyedFPIndex, ChunkFiler> skyHookIndexOpener;

    private final CreateFiler<Integer, MapContext, ChunkFiler> mapCreator;
    private final OpenFiler<MapContext, ChunkFiler> mapOpener;
    private final GrowFiler<Integer, MapContext, ChunkFiler> mapGrower;

    public TxNamedMap(TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyHookCog,
        int seed,
        ChunkStore chunkStore,
        long constantFP,
        CreateFiler<Integer, MapContext, ChunkFiler> mapCreator,
        OpenFiler<MapContext, ChunkFiler> mapOpener,
        GrowFiler<Integer, MapContext, ChunkFiler> mapGrower,
        IntIndexSemaphore skyHookKeySemaphores) {

        this.skyHookCog = skyHookCog;
        this.chunkStore = chunkStore;
        this.constantFP = constantFP;

        this.skyHookIndexCreator = new KeyedFPIndexCreator(seed, KeyedFPIndexCreator.DEFAULT_MAGIC_HEADER, KeyedFPIndexCreator.DEFAULT_MAX_KEY_SIZE_POWER,
            skyHookKeySemaphores);
        this.skyHookIndexOpener = new KeyedFPIndexOpener(seed, KeyedFPIndexCreator.DEFAULT_MAGIC_HEADER, KeyedFPIndexCreator.DEFAULT_MAX_KEY_SIZE_POWER,
            skyHookKeySemaphores);

        this.mapCreator = mapCreator;
        this.mapOpener = mapOpener;
        this.mapGrower = mapGrower;
    }

    private final MapBackedKeyedFPIndexGrower grower = new MapBackedKeyedFPIndexGrower();

    public <R> R readWriteAutoGrow(final byte[] mapName, int additionalCapacity, final ChunkTransaction<MapContext, R> mapTransaction) throws IOException {
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                long fp = chunkStore.newChunk(null, skyHookIndexCreator);
                checkState(fp == constantFP, "Must initialize to constantFP");
            }
        }
        return chunkStore.execute(constantFP, skyHookIndexOpener, (monkey, filer, lock) -> {

            int chunkPower = FilerIO.chunkPower(mapName.length, 0);
            return monkey.readWriteAutoGrow(chunkStore, chunkPower, 1, skyHookCog.creators[chunkPower], skyHookCog.opener, grower,
                (monkey1, filer1, lock1) -> monkey1.readWriteAutoGrow(chunkStore, mapName, additionalCapacity, mapCreator, mapOpener, mapGrower, mapTransaction));

        });
    }

    public <R> R read(final byte[] mapName, final ChunkTransaction<MapContext, R> mapTransaction) throws IOException {
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                return mapTransaction.commit(null, null, null);
            }
        }
        return chunkStore.execute(constantFP, skyHookIndexOpener, (monkey, filer, lock) -> {
            if (monkey == null || filer == null) {
                return mapTransaction.commit(null, null, null);
            }

            int chunkPower = FilerIO.chunkPower(mapName.length, 0);
            return monkey.read(chunkStore, chunkPower, skyHookCog.opener,
                (monkey1, filer1, lock1) -> {
                    if (monkey1 != null && filer1 != null) {
                        return monkey1.read(chunkStore, mapName, mapOpener, mapTransaction);
                    } else {
                        return mapTransaction.commit(null, null, null);
                    }
                });

        });
    }

    public Boolean stream(final byte[] mapName, final TxStream<byte[], MapContext, ChunkFiler> stream) throws IOException {
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                return true;
            }
        }
        return chunkStore.execute(constantFP, skyHookIndexOpener, (monkey, filer, lock) -> {
            if (monkey == null || filer == null) {
                return true;
            }

            int chunkPower = FilerIO.chunkPower(mapName.length, 0);
            return monkey.read(chunkStore, chunkPower, skyHookCog.opener,
                (skyHookMonkey, skyHookFiler, skyHookLock) -> {
                    if (skyHookMonkey == null || skyHookFiler == null) {
                        return true;
                    }
                    return skyHookMonkey.read(chunkStore, mapName, mapOpener, (mapMonkey, mapFiler, mapLock) -> {
                        if (mapMonkey == null || mapFiler == null) {
                            return true;
                        }
                        return stream.stream(mapName, mapMonkey, mapFiler, mapLock);
                    });
                });
        });
    }
}