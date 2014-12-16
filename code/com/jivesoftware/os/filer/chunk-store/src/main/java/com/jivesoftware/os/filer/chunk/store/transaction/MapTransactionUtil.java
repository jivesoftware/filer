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
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class MapTransactionUtil {

    public static final MapTransactionUtil INSTANCE = new MapTransactionUtil();

    private MapTransactionUtil() {
    }

    public <R> R read(final ChunkStore chunkStore,
        final KeyedFP<byte[]> parentStore,
        final byte[] key,
        final StoreTransaction<R, MapContextFiler> storeTransaction) throws IOException {
        long fp = parentStore.getFP(key);
        if (fp < 0) {
            return null;
        } else {
            return chunkStore.execute(fp,
                MapChunkOpener.INSTANCE,
                new ChunkTransaction<MapContext, R>() {
                    @Override
                    public R commit(MapContext mapContext, ChunkFiler filer) throws IOException {
                        R result = storeTransaction.commit(new MapContextFiler(mapContext, filer));
                        parentStore.update(key, filer.getChunkFP());
                        return result;
                    }
                });
        }
    }

    public <R, K> R write(final ChunkStore chunkStore,
        final int keySize,
        final boolean variableKeySize,
        final int payloadSize,
        final boolean variablePayloadSize,
        final int alwaysRoomForNMoreKeys,
        final KeyedFP<K> parentStore,
        final K key,
        final StoreTransaction<R, MapContextFiler> storeTransaction) throws IOException {

        long fp = parentStore.getFP(key);
        if (fp < 0) {
            int computeFilerSize = MapStore.INSTANCE.computeFilerSize(alwaysRoomForNMoreKeys, keySize, variableKeySize, payloadSize, variablePayloadSize);
            fp = chunkStore.newChunk(computeFilerSize, creator(keySize, variableKeySize, payloadSize, variablePayloadSize));
            R result = chunkStore.execute(fp,
                MapChunkOpener.INSTANCE,
                new ChunkTransaction<MapContext, R>() {
                    @Override
                    public R commit(MapContext mapContext, ChunkFiler filer) throws IOException {
                        return storeTransaction.commit(new MapContextFiler(mapContext, filer));
                    }
                });
            parentStore.update(key, fp);
            return result;
        } else {
            return chunkStore.execute(fp,
                MapChunkOpener.INSTANCE,
                new ChunkTransaction<MapContext, R>() {

                    @Override
                    public R commit(final MapContext context, final ChunkFiler filer) throws IOException {
                        if (MapStore.INSTANCE.isFullWithNMore(filer, context, alwaysRoomForNMoreKeys)) {
                            final int newSize = MapStore.INSTANCE.nextGrowSize(context, alwaysRoomForNMoreKeys);
                            int filerSize = MapStore.INSTANCE.computeFilerSize(newSize, context);
                            long newFP = chunkStore.newChunk(filerSize, new CreateFiler<MapContext, ChunkFiler>() {
                                @Override
                                public MapContext create(ChunkFiler newIndexFiler) throws IOException {
                                    MapContext newIndexChunk = MapStore.INSTANCE.create(newSize, context, newIndexFiler);
                                    MapStore.INSTANCE.copyTo(filer, context, newIndexFiler, newIndexChunk, null);
                                    return newIndexChunk;
                                }
                            });

                            R result = chunkStore.execute(newFP,
                                MapChunkOpener.INSTANCE,
                                new ChunkTransaction<MapContext, R>() {
                                    @Override
                                    public R commit(MapContext mapContext, ChunkFiler filer) throws IOException {
                                        return storeTransaction.commit(new MapContextFiler(mapContext, filer));
                                    }
                                });
                            parentStore.update(key, newFP);
                            chunkStore.remove(filer.getChunkFP());
                            return result;
                        } else {
                            return storeTransaction.commit(new MapContextFiler(context, filer));
                        }
                    }

                });
        }

    }

    CreateFiler<MapContext, ChunkFiler> creator(final int keySize, final boolean variableKeySizes,
        final int payloadSize, final boolean variablePayloadSize) {
        return new CreateFiler<MapContext, ChunkFiler>() {
            @Override
            public MapContext create(ChunkFiler mapStoresFiler) throws IOException {
                MapContext chunk = MapStore.INSTANCE.create(2, keySize, variableKeySizes, payloadSize, variablePayloadSize, mapStoresFiler);
                return chunk;
            }
        };
    }

}
