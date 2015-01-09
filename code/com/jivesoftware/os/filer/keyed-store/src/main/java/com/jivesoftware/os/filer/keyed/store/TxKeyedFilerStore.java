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
package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkTransaction;
import com.jivesoftware.os.filer.chunk.store.RewriteChunkTransaction;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMapOfFiler;
import com.jivesoftware.os.filer.chunk.store.transaction.TxPartitionedNamedMapOfFiler;
import com.jivesoftware.os.filer.chunk.store.transaction.TxStream;
import com.jivesoftware.os.filer.chunk.store.transaction.TxStreamKeys;
import com.jivesoftware.os.filer.io.ByteArrayPartitionFunction;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerLock;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpGrowFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class TxKeyedFilerStore implements KeyedFilerStore {

    static final long SKY_HOOK_FP = 464; // I died a little bit doing this.

    private final byte[] name;
    private final TxPartitionedNamedMapOfFiler<FilerLock> namedMapOfFilers;

    public TxKeyedFilerStore(ChunkStore[] chunkStores, byte[] name) {
        // TODO consider replacing with builder pattern
        @SuppressWarnings("unchecked")
        TxNamedMapOfFiler<FilerLock>[] stores = new TxNamedMapOfFiler[chunkStores.length];
        for (int i = 0; i < stores.length; i++) {
            stores[i] = new TxNamedMapOfFiler<>(chunkStores[i], SKY_HOOK_FP,
                new NoOpCreateFiler<ChunkFiler>(), new NoOpOpenFiler<ChunkFiler>(), new NoOpGrowFiler<Long, FilerLock, ChunkFiler>());
        }

        this.name = name;
        this.namedMapOfFilers = new TxPartitionedNamedMapOfFiler<>(new ByteArrayPartitionFunction(), stores);
    }

    @Override
    public <R> R execute(byte[] keyBytes, long newFilerInitialCapacity, final FilerTransaction<Filer, R> transaction) throws IOException {
        if (newFilerInitialCapacity < 0) {
            return namedMapOfFilers.read(keyBytes, name, keyBytes, new ChunkTransaction<FilerLock, R>() {

                @Override
                public R commit(FilerLock monkey, ChunkFiler filer) throws IOException {
                    if (monkey == null || filer == null) {
                        return transaction.commit(null);
                    }
                    synchronized (monkey) {
                        return transaction.commit(filer);
                    }
                }
            });
        } else {
            return namedMapOfFilers.overwrite(keyBytes, name, keyBytes, newFilerInitialCapacity, new ChunkTransaction<FilerLock, R>() {

                @Override
                public R commit(FilerLock monkey, ChunkFiler filer) throws IOException {
                    synchronized (monkey) {
                        return transaction.commit(filer);
                    }
                }
            });
        }
    }

    @Override
    public <R> R executeRewrite(byte[] keyBytes, long newFilerInitialCapacity, final RewriteFilerTransaction<Filer, R> transaction) throws IOException {
        if (newFilerInitialCapacity < 0) {
            throw new IllegalArgumentException("newFilerInitialCapacity must be greater than -1");
        } else {
            return namedMapOfFilers.rewrite(keyBytes, name, keyBytes, newFilerInitialCapacity, new RewriteChunkTransaction<FilerLock, R>() {

                @Override
                public R commit(FilerLock currentMonkey, ChunkFiler currentFiler, FilerLock newMonkey, ChunkFiler newFiler) throws IOException {
                    synchronized (currentMonkey) {
                        synchronized (newMonkey) {
                            return transaction.commit(currentFiler, newFiler);
                        }
                    }
                }
            });
        }
    }

    @Override
    public boolean stream(final KeyValueStore.EntryStream<IBA, Filer> stream) throws IOException {
        return namedMapOfFilers.stream(name, new TxStream<byte[], FilerLock, ChunkFiler>() {

            @Override
            public boolean stream(byte[] key, FilerLock monkey, ChunkFiler filer) throws IOException {
                synchronized (monkey) {
                    return stream.stream(new IBA(key), filer);
                }
            }
        });
    }

    @Override
    public boolean streamKeys(final KeyValueStore.KeyStream<IBA> stream) throws IOException {
        return namedMapOfFilers.streamKeys(name, new TxStreamKeys<byte[]>() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                return stream.stream(new IBA(key));
            }
        });
    }

    @Override
    public void close() {
    }

}
