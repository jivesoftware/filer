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
import com.jivesoftware.os.filer.chunk.store.RewriteChunkTransaction;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class TxNamedMapOfFiler<M> {

    private static final MapBackedKeyedFPIndexCreator[] POWER_CREATORS = new MapBackedKeyedFPIndexCreator[16];

    static {
        for (int i = 0; i < POWER_CREATORS.length; i++) {
            POWER_CREATORS[i] = new MapBackedKeyedFPIndexCreator(2, (int) FilerIO.chunkLength(i), true, 8, false);
        }
    }

    private final ChunkStore chunkStore;
    private final long constantFP;
    private final CreateFiler<Long, M, ChunkFiler> filerCreator;
    private final OpenFiler<M, ChunkFiler> filerOpener;
    private final GrowFiler<Long, M, ChunkFiler> filerGrower;

    private final MapBackedKeyedFPIndexGrower grower = new MapBackedKeyedFPIndexGrower(1);

    public TxNamedMapOfFiler(ChunkStore chunkStore,
        long constantFP,
        CreateFiler<Long, M, ChunkFiler> filerCreator,
        OpenFiler<M, ChunkFiler> filerOpener,
        GrowFiler<Long, M, ChunkFiler> filerGrower) {
        this.chunkStore = chunkStore;
        this.constantFP = constantFP;
        this.filerCreator = filerCreator;
        this.filerOpener = filerOpener;
        this.filerGrower = filerGrower;
    }

    public <R> R overwrite(final byte[] mapName,
        final byte[] filerKey,
        final Long sizeHint,
        final ChunkTransaction<M, R> filerTransaction) throws IOException {

        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                chunkStore.newChunk(null, KeyedFPIndexCreator.DEFAULT);
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                MapBackedKeyedFPIndexCreator creator = POWER_CREATORS[chunkPower];
                return monkey.commit(chunkStore, chunkPower, 2, creator, MapBackedKeyedFPIndexOpener.INSTANCE,
                    grower, new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                        @Override
                        public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {

                            return monkey.commit(chunkStore, mapName, null, KeyedFPIndexCreator.DEFAULT, KeyedFPIndexOpener.DEFAULT, null,
                                new ChunkTransaction<PowerKeyedFPIndex, R>() {
                                    @Override
                                    public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                                        int chunkPower = FilerIO.chunkPower(filerKey.length, 0);
                                        MapBackedKeyedFPIndexCreator creator = POWER_CREATORS[chunkPower];
                                        return monkey.commit(chunkStore, chunkPower, 2, creator, MapBackedKeyedFPIndexOpener.INSTANCE, grower,
                                            new ChunkTransaction<MapBackedKeyedFPIndex, R>() {
                                                @Override
                                                public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                                                    // TODO consider using the provided filer in appropriate cases.
                                                    return monkey.commit(chunkStore, filerKey, sizeHint, filerCreator, filerOpener, filerGrower,
                                                        filerTransaction);
                                                }
                                            });
                                    }
                                });
                        }
                    });
            }
        });
    }

    public <R> R rewrite(final byte[] mapName,
        final byte[] filerKey,
        final Long sizeHint,
        final RewriteChunkTransaction<M, R> rewriteChunkTransaction) throws IOException {

        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                chunkStore.newChunk(null, KeyedFPIndexCreator.DEFAULT);
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                MapBackedKeyedFPIndexCreator creator = POWER_CREATORS[chunkPower];
                return monkey.commit(chunkStore, chunkPower, 2, creator, MapBackedKeyedFPIndexOpener.INSTANCE,
                    grower, new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                        @Override
                        public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {

                            return monkey.commit(chunkStore, mapName, null, KeyedFPIndexCreator.DEFAULT, KeyedFPIndexOpener.DEFAULT, null,
                                new ChunkTransaction<PowerKeyedFPIndex, R>() {
                                    @Override
                                    public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                                        int chunkPower = FilerIO.chunkPower(filerKey.length, 0);
                                        MapBackedKeyedFPIndexCreator creator = POWER_CREATORS[chunkPower];
                                        return monkey.commit(chunkStore, chunkPower, 1, creator, MapBackedKeyedFPIndexOpener.INSTANCE, grower,
                                            new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                                                @Override
                                                public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                                                    // TODO consider using the provided filer in appropriate cases.
                                                    final AtomicReference<R> result = new AtomicReference<>();
                                                    GrowFiler<Long, M, ChunkFiler> rewriteGrower = new GrowFiler<Long, M, ChunkFiler>() {

                                                        @Override
                                                        public Long acquire(M monkey, ChunkFiler filer) throws IOException {
                                                            return sizeHint;
                                                        }

                                                        @Override
                                                        public void growAndAcquire(M currentMonkey,
                                                            ChunkFiler currentFiler,
                                                            M newMonkey,
                                                            ChunkFiler newFiler) throws IOException {
                                                            filerGrower.growAndAcquire(currentMonkey, currentFiler, newMonkey, newFiler);
                                                            result.set(rewriteChunkTransaction.commit(currentMonkey, currentFiler, newMonkey, newFiler));
                                                        }

                                                        @Override
                                                        public void release(M monkey) {
                                                        }
                                                    };
                                                    return monkey.commit(chunkStore, filerKey, sizeHint, filerCreator, filerOpener, rewriteGrower,
                                                        new ChunkTransaction<M, R>() {

                                                            @Override
                                                            public R commit(M monkey, ChunkFiler filer) throws IOException {
                                                                return result.get();
                                                            }
                                                        });
                                                }
                                            });
                                    }
                                });
                        }
                    });
            }
        });
    }

    public <R> R read(final byte[] mapName, final byte[] filerKey, final ChunkTransaction<M, R> filerTransaction) throws IOException {
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                return filerTransaction.commit(null, null);
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                if (monkey == null || filer == null) {
                    return filerTransaction.commit(null, null);
                }

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.commit(chunkStore, chunkPower, 1, null, MapBackedKeyedFPIndexOpener.INSTANCE, null,
                    new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                        @Override
                        public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                            if (monkey == null || filer == null) {
                                return filerTransaction.commit(null, null);
                            }

                            return monkey.commit(chunkStore, mapName, null, null, KeyedFPIndexOpener.DEFAULT, null,
                                new ChunkTransaction<PowerKeyedFPIndex, R>() {
                                    @Override
                                    public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                                        if (monkey == null || filer == null) {
                                            return filerTransaction.commit(null, null);
                                        }

                                        int chunkPower = FilerIO.chunkPower(filerKey.length, 0);
                                        return monkey.commit(chunkStore, chunkPower, 1, null, MapBackedKeyedFPIndexOpener.INSTANCE, null,
                                            new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                                                @Override
                                                public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                                                    if (monkey == null || filer == null) {
                                                        return filerTransaction.commit(null, null);
                                                    }
                                                    // TODO consider using the provided filer in appropriate cases.
                                                    return monkey.commit(chunkStore, filerKey, null, null, filerOpener, null, filerTransaction);
                                                }
                                            });
                                    }
                                });
                        }
                    });
            }
        });
    }

    public Boolean stream(final byte[] mapName, final TxStream<byte[], M, ChunkFiler> stream) throws IOException {
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                return true;
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, Boolean>() {

            @Override
            public Boolean commit(final PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                if (monkey == null || filer == null) {
                    return true;
                }

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.commit(chunkStore, chunkPower, null, null, MapBackedKeyedFPIndexOpener.INSTANCE, null,
                    new ChunkTransaction<MapBackedKeyedFPIndex, Boolean>() {

                        @Override
                        public Boolean commit(final MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                            if (monkey == null || filer == null) {
                                return true;
                            }

                            return monkey.commit(chunkStore, mapName, null, null, KeyedFPIndexOpener.DEFAULT, null,
                                new ChunkTransaction<PowerKeyedFPIndex, Boolean>() {

                                    @Override
                                    public Boolean commit(final PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                                        if (monkey == null || filer == null) {
                                            return true;
                                        }
                                        return monkey.stream(new KeysStream<Integer>() {

                                            @Override
                                            public boolean stream(Integer key) throws IOException {
                                                Boolean result = monkey.commit(chunkStore, key, null, null, MapBackedKeyedFPIndexOpener.INSTANCE, null,
                                                    new ChunkTransaction<MapBackedKeyedFPIndex, Boolean>() {

                                                        @Override
                                                        public Boolean commit(final MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                                                            if (monkey == null || filer == null) {
                                                                return true;
                                                            }
                                                            return monkey.stream(new KeysStream<byte[]>() {

                                                                @Override
                                                                public boolean stream(final byte[] key) throws IOException {
                                                                    Boolean result = monkey.commit(chunkStore, key, null, null, filerOpener, null,
                                                                        new ChunkTransaction<M, Boolean>() {

                                                                            @Override
                                                                            public Boolean commit(M monkey, ChunkFiler filer) throws IOException {
                                                                                return stream.stream(key, monkey, filer);
                                                                            }
                                                                        });
                                                                    return result;
                                                                }
                                                            });
                                                        }
                                                    });
                                                return result;
                                            }
                                        });
                                    }
                                });

                        }
                    });
            }
        });
    }

    public Boolean streamKeys(final byte[] mapName, final TxStreamKeys<byte[]> stream) throws IOException {
        synchronized (chunkStore) {
            if (!chunkStore.isValid(constantFP)) {
                return true;
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, Boolean>() {

            @Override
            public Boolean commit(final PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                if (monkey == null || filer == null) {
                    return true;
                }

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.commit(chunkStore, chunkPower, null, null, MapBackedKeyedFPIndexOpener.INSTANCE, null,
                    new ChunkTransaction<MapBackedKeyedFPIndex, Boolean>() {

                        @Override
                        public Boolean commit(final MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                            if (monkey == null || filer == null) {
                                return true;
                            }

                            return monkey.commit(chunkStore, mapName, null, null, KeyedFPIndexOpener.DEFAULT, null,
                                new ChunkTransaction<PowerKeyedFPIndex, Boolean>() {

                                    @Override
                                    public Boolean commit(final PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                                        if (monkey == null || filer == null) {
                                            return true;
                                        }
                                        return monkey.stream(new KeysStream<Integer>() {

                                            @Override
                                            public boolean stream(Integer key) throws IOException {
                                                Boolean result = monkey.commit(chunkStore, key, null, null, MapBackedKeyedFPIndexOpener.INSTANCE, null,
                                                    new ChunkTransaction<MapBackedKeyedFPIndex, Boolean>() {

                                                        @Override
                                                        public Boolean commit(final MapBackedKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                                                            if (monkey == null || filer == null) {
                                                                return true;
                                                            }
                                                            Boolean result = monkey.stream(new KeysStream<byte[]>() {

                                                                @Override
                                                                public boolean stream(final byte[] key) throws IOException {
                                                                    return stream.stream(key);
                                                                }
                                                            });
                                                            return result;
                                                        }
                                                    });
                                                return result;
                                            }
                                        });
                                    }
                                });

                        }
                    });
            }
        });

    }
}
