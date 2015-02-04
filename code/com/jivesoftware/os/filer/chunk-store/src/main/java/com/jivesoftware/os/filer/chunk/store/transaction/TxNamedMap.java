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
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.map.store.MapContext;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;
import static com.jivesoftware.os.filer.chunk.store.transaction.TxPowerConstants.SKY_HOOK_POWER_CREATORS;
import static com.jivesoftware.os.filer.chunk.store.transaction.TxPowerConstants.SKY_HOOK_POWER_OPENER;

/**
 * @author jonathan.colt
 */
public class TxNamedMap {

    private final ChunkStore chunkStore;
    private final long constantFP;

    private final CreateFiler<Integer, MapContext, ChunkFiler> mapCreator;
    private final OpenFiler<MapContext, ChunkFiler> mapOpener;
    private final GrowFiler<Integer, MapContext, ChunkFiler> mapGrower;

    public TxNamedMap(ChunkStore chunkStore,
        long constantFP,
        CreateFiler<Integer, MapContext, ChunkFiler> mapCreator,
        OpenFiler<MapContext, ChunkFiler> mapOpener,
        GrowFiler<Integer, MapContext, ChunkFiler> mapGrower) {
        this.chunkStore = chunkStore;
        this.constantFP = constantFP;
        this.mapCreator = mapCreator;
        this.mapOpener = mapOpener;
        this.mapGrower = mapGrower;

    }

    private final MapBackedKeyedFPIndexGrower grower = new MapBackedKeyedFPIndexGrower(1);

    public <R> R readWriteAutoGrow(final byte[] mapName, final ChunkTransaction<MapContext, R> mapTransaction) throws IOException {
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                long fp = chunkStore.newChunk(null, KeyedFPIndexCreator.DEFAULT);
                checkState(fp == constantFP, "Must initialize to constantFP");
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.readWriteAutoGrow(chunkStore, chunkPower, 1, SKY_HOOK_POWER_CREATORS[chunkPower], SKY_HOOK_POWER_OPENER, grower,
                    new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                        @Override
                        public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                            return monkey.readWriteAutoGrow(chunkStore, mapName, 1, mapCreator, mapOpener, mapGrower, mapTransaction);
                        }

                    });

            }
        });
    }

    public <R> R read(final byte[] mapName, final ChunkTransaction<MapContext, R> mapTransaction) throws IOException {
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                return mapTransaction.commit(null, null, null);
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                if (monkey == null || filer == null) {
                    return mapTransaction.commit(null, null, null);
                }

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.read(chunkStore, chunkPower, SKY_HOOK_POWER_OPENER,
                    new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                        @Override
                        public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                            if (monkey != null && filer != null) {
                                return monkey.read(chunkStore, mapName, mapOpener, mapTransaction);
                            } else {
                                return mapTransaction.commit(null, null, null);
                            }
                        }

                    });

            }
        });
    }

    public Boolean stream(final byte[] mapName, final TxStream<byte[], MapContext, ChunkFiler> stream) throws IOException {
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                return true;
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, Boolean>() {

            @Override
            public Boolean commit(final PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                if (monkey == null || filer == null) {
                    return true;
                }

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.read(chunkStore, chunkPower, SKY_HOOK_POWER_OPENER,
                    new ChunkTransaction<MapBackedKeyedFPIndex, Boolean>() {

                        @Override
                        public Boolean commit(final MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                            if (monkey == null || filer == null) {
                                return true;
                            }
                            return monkey.read(chunkStore, mapName, mapOpener, new ChunkTransaction<MapContext, Boolean>() {

                                @Override
                                public Boolean commit(MapContext monkey, ChunkFiler filer, Object lock) throws IOException {
                                    if (monkey == null || filer == null) {
                                        return true;
                                    }
                                    return stream.stream(mapName, monkey, filer, lock);
                                }
                            });
                        }
                    });
            }
        });
    }
}
