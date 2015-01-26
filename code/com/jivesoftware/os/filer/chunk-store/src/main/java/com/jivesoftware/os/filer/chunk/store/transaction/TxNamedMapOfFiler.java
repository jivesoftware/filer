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
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.map.store.api.KeyRange;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.jivesoftware.os.filer.chunk.store.transaction.TxPowerConstants.SKY_HOOK_POWER_CREATORS;
import static com.jivesoftware.os.filer.chunk.store.transaction.TxPowerConstants.SKY_HOOK_POWER_GROWER;
import static com.jivesoftware.os.filer.chunk.store.transaction.TxPowerConstants.SKY_HOOK_POWER_OPENER;

/**
 * @author jonathan.colt
 */
public class TxNamedMapOfFiler<N extends FPIndex<byte[], N>, M> {

    public static final NoOpCreateFiler<ChunkFiler> CHUNK_FILER_CREATOR = new NoOpCreateFiler<>();
    public static final NoOpOpenFiler<ChunkFiler> CHUNK_FILER_OPENER = new NoOpOpenFiler<>();

    private final ChunkStore chunkStore;
    private final long constantFP;

    private final CreateFiler<Integer, N, ChunkFiler>[] namedPowerCreator;
    private final OpenFiler<N, ChunkFiler> namedPowerOpener;
    private final GrowFiler<Integer, N, ChunkFiler> namedPowerGrower;
    private final CreateFiler<Long, M, ChunkFiler> filerCreator;
    private final OpenFiler<M, ChunkFiler> filerOpener;

    public TxNamedMapOfFiler(
        ChunkStore chunkStore,
        long constantFP,
        CreateFiler<Integer, N, ChunkFiler>[] namedPowerCreator,
        OpenFiler<N, ChunkFiler> namedPowerOpener,
        GrowFiler<Integer, N, ChunkFiler> namedPowerGrower,
        CreateFiler<Long, M, ChunkFiler> filerCreator,
        OpenFiler<M, ChunkFiler> filerOpener) {
        this.chunkStore = chunkStore;
        this.constantFP = constantFP;
        this.namedPowerCreator = namedPowerCreator;
        this.namedPowerOpener = namedPowerOpener;
        this.namedPowerGrower = namedPowerGrower;
        this.filerCreator = filerCreator;
        this.filerOpener = filerOpener;
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
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.commit(chunkStore, chunkPower, 2, SKY_HOOK_POWER_CREATORS[chunkPower], SKY_HOOK_POWER_OPENER,
                    SKY_HOOK_POWER_GROWER, new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                        @Override
                        public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {

                            return monkey.commit(chunkStore, mapName, null, KeyedFPIndexCreator.DEFAULT, KeyedFPIndexOpener.DEFAULT, null,
                                new ChunkTransaction<PowerKeyedFPIndex, R>() {
                                    @Override
                                    public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                                        int chunkPower = FilerIO.chunkPower(filerKey.length, 0);
                                        return monkey.commit(chunkStore, chunkPower, 2, namedPowerCreator[chunkPower], namedPowerOpener, namedPowerGrower,
                                            new ChunkTransaction<N, R>() {
                                                @Override
                                                public R commit(N monkey, ChunkFiler filer, Object lock) throws IOException {
                                                    // TODO consider using the provided filer in appropriate cases.
                                                    GrowFiler<Long, M, ChunkFiler> overwriteGrower = new GrowFiler<Long, M, ChunkFiler>() {

                                                        @Override
                                                        public Long acquire(M monkey, ChunkFiler filer, Object lock) throws IOException {
                                                            return filer.length() < sizeHint ? sizeHint : null;
                                                        }

                                                        @Override
                                                        public void growAndAcquire(M currentMonkey,
                                                            ChunkFiler currentFiler,
                                                            M newMonkey,
                                                            ChunkFiler newFiler,
                                                            Object currentLock,
                                                            Object newLock) throws IOException {
                                                            synchronized (currentLock) {
                                                                synchronized (newLock) {
                                                                    currentFiler.seek(0);
                                                                    newFiler.seek(0);
                                                                    FilerIO.copy(currentFiler, newFiler, -1);
                                                                }
                                                            }
                                                        }

                                                        @Override
                                                        public void release(M monkey, Object lock) {
                                                        }
                                                    };
                                                    return monkey.commit(chunkStore, filerKey, sizeHint, filerCreator, filerOpener, overwriteGrower,
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
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.commit(chunkStore, chunkPower, 2, SKY_HOOK_POWER_CREATORS[chunkPower], SKY_HOOK_POWER_OPENER, SKY_HOOK_POWER_GROWER,
                    new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                        @Override
                        public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {

                            return monkey.commit(chunkStore, mapName, null, KeyedFPIndexCreator.DEFAULT, KeyedFPIndexOpener.DEFAULT, null,
                                new ChunkTransaction<PowerKeyedFPIndex, R>() {
                                    @Override
                                    public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                                        int chunkPower = FilerIO.chunkPower(filerKey.length, 0);
                                        return monkey.commit(chunkStore, chunkPower, 1, namedPowerCreator[chunkPower], namedPowerOpener, namedPowerGrower,
                                            new ChunkTransaction<N, R>() {

                                                @Override
                                                public R commit(N monkey, ChunkFiler filer, Object lock) throws IOException {
                                                    // TODO consider using the provided filer in appropriate cases.
                                                    final AtomicReference<R> result = new AtomicReference<>();
                                                    GrowFiler<Long, M, ChunkFiler> rewriteGrower = new GrowFiler<Long, M, ChunkFiler>() {

                                                        @Override
                                                        public Long acquire(M monkey, ChunkFiler filer, Object lock) throws IOException {
                                                            return sizeHint;
                                                        }

                                                        @Override
                                                        public void growAndAcquire(M currentMonkey,
                                                            ChunkFiler currentFiler,
                                                            M newMonkey,
                                                            ChunkFiler newFiler,
                                                            Object currentLock,
                                                            Object newLock) throws IOException {
                                                            result.set(rewriteChunkTransaction.commit(currentMonkey, currentFiler, newMonkey, newFiler,
                                                                    currentLock, newLock));
                                                        }

                                                        @Override
                                                        public void release(M monkey, Object lock) {
                                                        }
                                                    };
                                                    return monkey.commit(chunkStore, filerKey, sizeHint, filerCreator, filerOpener, rewriteGrower,
                                                        new ChunkTransaction<M, R>() {

                                                            @Override
                                                            public R commit(M monkey, ChunkFiler filer, Object lock) throws IOException {
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
                return filerTransaction.commit(null, null, null);
            }
        }
        return chunkStore.execute(constantFP, KeyedFPIndexOpener.DEFAULT, new ChunkTransaction<PowerKeyedFPIndex, R>() {

            @Override
            public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                if (monkey == null || filer == null) {
                    return filerTransaction.commit(null, null, null);
                }

                int chunkPower = FilerIO.chunkPower(mapName.length, 0);
                return monkey.commit(chunkStore, chunkPower, 1, null, SKY_HOOK_POWER_OPENER, null,
                    new ChunkTransaction<MapBackedKeyedFPIndex, R>() {

                        @Override
                        public R commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                            if (monkey == null || filer == null) {
                                return filerTransaction.commit(null, null, null);
                            }

                            return monkey.commit(chunkStore, mapName, null, null, KeyedFPIndexOpener.DEFAULT, null,
                                new ChunkTransaction<PowerKeyedFPIndex, R>() {
                                    @Override
                                    public R commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                                        if (monkey == null || filer == null) {
                                            return filerTransaction.commit(null, null, null);
                                        }

                                        int chunkPower = FilerIO.chunkPower(filerKey.length, 0);
                                        return monkey.commit(chunkStore, chunkPower, 1, null, namedPowerOpener, null,
                                            new ChunkTransaction<N, R>() {

                                                @Override
                                                public R commit(N monkey, ChunkFiler filer, Object lock) throws IOException {
                                                    if (monkey == null || filer == null) {
                                                        return filerTransaction.commit(null, null, null);
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

    public Boolean stream(final byte[] mapName, final List<KeyRange> ranges, final TxStream<byte[], M, ChunkFiler> stream) throws IOException {
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
                return monkey.commit(chunkStore, chunkPower, null, null, SKY_HOOK_POWER_OPENER, null,
                    new ChunkTransaction<MapBackedKeyedFPIndex, Boolean>() {

                        @Override
                        public Boolean commit(final MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                            if (monkey == null || filer == null) {
                                return true;
                            }

                            return monkey.commit(chunkStore, mapName, null, null, KeyedFPIndexOpener.DEFAULT, null,
                                new ChunkTransaction<PowerKeyedFPIndex, Boolean>() {

                                    @Override
                                    public Boolean commit(final PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                                        if (monkey == null || filer == null) {
                                            return true;
                                        }
                                        return monkey.stream(null, new KeysStream<Integer>() {

                                            @Override
                                            public boolean stream(Integer key) throws IOException {
                                                Boolean result = monkey.commit(chunkStore, key, null, null, namedPowerOpener, null,
                                                    new ChunkTransaction<N, Boolean>() {

                                                        @Override
                                                        public Boolean commit(final N monkey, ChunkFiler filer, Object lock)
                                                        throws IOException {
                                                            if (monkey == null || filer == null) {
                                                                return true;
                                                            }
                                                            return monkey.stream(ranges, new KeysStream<byte[]>() {

                                                                @Override
                                                                public boolean stream(final byte[] key) throws IOException {
                                                                    Boolean result = monkey.commit(chunkStore, key, null, null, filerOpener, null,
                                                                        new ChunkTransaction<M, Boolean>() {

                                                                            @Override
                                                                            public Boolean commit(M monkey, ChunkFiler filer, Object lock) throws IOException {
                                                                                return stream.stream(key, monkey, filer, lock);
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

    public Boolean streamKeys(final byte[] mapName, final List<KeyRange> ranges, final TxStreamKeys<byte[]> stream) throws IOException {
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
                return monkey.commit(chunkStore, chunkPower, null, null, SKY_HOOK_POWER_OPENER, null,
                    new ChunkTransaction<MapBackedKeyedFPIndex, Boolean>() {

                        @Override
                        public Boolean commit(final MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                            if (monkey == null || filer == null) {
                                return true;
                            }

                            return monkey.commit(chunkStore, mapName, null, null, KeyedFPIndexOpener.DEFAULT, null,
                                new ChunkTransaction<PowerKeyedFPIndex, Boolean>() {

                                    @Override
                                    public Boolean commit(final PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                                        if (monkey == null || filer == null) {
                                            return true;
                                        }
                                        return monkey.stream(null, new KeysStream<Integer>() {

                                            @Override
                                            public boolean stream(Integer key) throws IOException {
                                                Boolean result = monkey.commit(chunkStore, key, null, null, namedPowerOpener, null,
                                                    new ChunkTransaction<N, Boolean>() {

                                                        @Override
                                                        public Boolean commit(final N monkey, ChunkFiler filer, Object lock)
                                                        throws IOException {
                                                            if (monkey == null || filer == null) {
                                                                return true;
                                                            }
                                                            Boolean result = monkey.stream(ranges, new KeysStream<byte[]>() {

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
