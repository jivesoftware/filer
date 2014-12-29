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
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.PartitionFunction;
import com.jivesoftware.os.filer.map.store.MapContext;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class TxNamedMap {

    private final ChunkStore[] chunkStores;
    private final long constantFP;
    private final PartitionFunction<byte[]> partitionFunction;

    private final MapBackedKeyedFPIndexOpener opener = new MapBackedKeyedFPIndexOpener();
    private final CreateFiler<Integer, MapContext, ChunkFiler> mapCreator;
    private final OpenFiler<MapContext, ChunkFiler> mapOpener;
    private final GrowFiler<Integer, MapContext, ChunkFiler> mapGrower;

    public TxNamedMap(ChunkStore[] chunkStores,
        long constantFP,
        PartitionFunction<byte[]> partitionFunction,
        CreateFiler<Integer, MapContext, ChunkFiler> mapCreator,
        OpenFiler<MapContext, ChunkFiler> mapOpener,
        GrowFiler<Integer, MapContext, ChunkFiler> mapGrower) {
        this.chunkStores = chunkStores;
        this.constantFP = constantFP;
        this.partitionFunction = partitionFunction;
        this.mapCreator = mapCreator;
        this.mapOpener = mapOpener;
        this.mapGrower = mapGrower;

    }

    private final MapBackedKeyedFPIndexGrower grower = new MapBackedKeyedFPIndexGrower(1);

    public <R> R write(byte[] partitionKey, final byte[] mapName, final ChunkTransaction<MapContext, R> mapTransaction) throws IOException {
        int i = partitionFunction.partition(chunkStores.length, partitionKey);
        final ChunkStore chunkStore = chunkStores[i];
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                chunkStore.newChunk(null, KeyedFPIndexCreator.DEFAULT);
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {

                MapBackedKeyedFPIndexCreator creator = new MapBackedKeyedFPIndexCreator(2, mapName.length, false, 8, false);
                return monkey.commit(chunkStore, mapName.length, 1, creator, opener, grower, new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                    @Override
                    public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                        return monkey.commit(chunkStore, mapName, 1, mapCreator, mapOpener, mapGrower, mapTransaction);
                    }

                });

            }
        });
    }

    public <R> R read(byte[] partitionKey, final byte[] mapName, final ChunkTransaction<MapContext, R> mapTransaction) throws IOException {
        int i = partitionFunction.partition(chunkStores.length, partitionKey);
        final ChunkStore chunkStore = chunkStores[i];
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                return null;
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {

                return monkey.commit(chunkStore, mapName.length, 1, null, opener, null, new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                    @Override
                    public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                        MapOpener opener = new MapOpener();
                        return monkey.commit(chunkStore, mapName, 1, null, opener, null, mapTransaction);
                    }

                });

            }
        });
    }

    public Boolean stream(final byte[] name, final TxStream<byte[], MapContext, ChunkFiler> stream) throws IOException {
        for (final ChunkStore chunkStore : chunkStores) {
            synchronized (chunkStore) {
                if (!chunkStore.isValid(constantFP)) {
                    continue;
                }
            }
            if (!chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, Boolean>() {

                @Override
                public Boolean commit(final PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {

                    return monkey.commit(chunkStore, name.length, null, null, opener, null, new ChunkTransaction<MapBackedKeyedFPIndex, Boolean>() {

                        @Override
                        public Boolean commit(final MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                            return monkey.commit(chunkStore, name, null, null, new MapOpener(), null, new ChunkTransaction<MapContext, Boolean>() {

                                @Override
                                public Boolean commit(MapContext monkey, ChunkFiler filer) throws IOException {
                                    return stream.stream(name, monkey, filer);
                                }
                            });
                        }
                    });
                }
            })) {
                return false;
            }
        }
        return true;
    }
}
