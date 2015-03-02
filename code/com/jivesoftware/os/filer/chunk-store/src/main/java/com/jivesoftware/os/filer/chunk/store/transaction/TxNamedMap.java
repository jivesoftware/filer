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

    private final TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyhookCog;
    private final CreateFiler<Void, PowerKeyedFPIndex, ChunkFiler> indexCreator;
    private final OpenFiler<PowerKeyedFPIndex, ChunkFiler> indexOpener;

    private final CreateFiler<Integer, MapContext, ChunkFiler> mapCreator;
    private final OpenFiler<MapContext, ChunkFiler> mapOpener;
    private final GrowFiler<Integer, MapContext, ChunkFiler> mapGrower;

    public TxNamedMap(TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyhookCog,
        int seed,
        ChunkStore chunkStore,
        long constantFP,
        CreateFiler<Integer, MapContext, ChunkFiler> mapCreator,
        OpenFiler<MapContext, ChunkFiler> mapOpener,
        GrowFiler<Integer, MapContext, ChunkFiler> mapGrower) {
        this.skyhookCog = skyhookCog;
        this.chunkStore = chunkStore;
        this.constantFP = constantFP;

        this.indexCreator = new KeyedFPIndexCreator(seed, KeyedFPIndexCreator.DEFAULT_MAGIC_HEADER, KeyedFPIndexCreator.DEFAULT_MAX_KEY_SIZE_POWER);
        this.indexOpener = new KeyedFPIndexOpener(seed, KeyedFPIndexCreator.DEFAULT_MAGIC_HEADER, KeyedFPIndexCreator.DEFAULT_MAX_KEY_SIZE_POWER);

        this.mapCreator = mapCreator;
        this.mapOpener = mapOpener;
        this.mapGrower = mapGrower;
    }

    private final MapBackedKeyedFPIndexGrower grower = new MapBackedKeyedFPIndexGrower(1);

    public <R> R readWriteAutoGrow(final byte[] mapName, final ChunkTransaction<MapContext, R> mapTransaction) throws IOException {
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                long fp = chunkStore.newChunk(null, indexCreator);
                checkState(fp == constantFP, "Must initialize to constantFP");
            }
        }
        return chunkStore.execute(constantFP, indexOpener, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.readWriteAutoGrow(chunkStore, chunkPower, 1, skyhookCog.creators[chunkPower], skyhookCog.opener, grower,
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
        return chunkStore.execute(constantFP, indexOpener, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                if (monkey == null || filer == null) {
                    return mapTransaction.commit(null, null, null);
                }

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.read(chunkStore, chunkPower, skyhookCog.opener,
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
        return chunkStore.execute(constantFP, indexOpener, new ChunkTransaction<PowerKeyedFPIndex, Boolean>() {

            @Override
            public Boolean commit(final PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                if (monkey == null || filer == null) {
                    return true;
                }

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.read(chunkStore, chunkPower, skyhookCog.opener,
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