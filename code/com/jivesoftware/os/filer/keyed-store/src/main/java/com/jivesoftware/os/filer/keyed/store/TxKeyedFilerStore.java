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

import com.jivesoftware.os.filer.chunk.store.transaction.MapBackedKeyedFPIndex;
import com.jivesoftware.os.filer.chunk.store.transaction.SkipListMapBackedKeyedFPIndex;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCog;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMapOfFiler;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMapOfFilerOverwriteGrowerProvider;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMapOfFilerRewriteGrowerProvider;
import com.jivesoftware.os.filer.chunk.store.transaction.TxPartitionedNamedMapOfFiler;
import com.jivesoftware.os.filer.chunk.store.transaction.TxStream;
import com.jivesoftware.os.filer.chunk.store.transaction.TxStreamKeys;
import com.jivesoftware.os.filer.io.ByteArrayPartitionFunction;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import java.io.IOException;
import java.util.List;

/**
 * @author jonathan.colt
 * @param <H>
 * @param <M>
 */
public class TxKeyedFilerStore<H, M> implements KeyedFilerStore<H, M> {

    static final long SKY_HOOK_FP = 464; // I died a little bit doing this.

    private final byte[] name;
    private final TxPartitionedNamedMapOfFiler<?, H, M> namedMapOfFilers;

    public TxKeyedFilerStore(TxCogs cogs, int seed, ChunkStore[] chunkStores, byte[] name, boolean lexOrderKeys,
        CreateFiler<H, M, ChunkFiler> filerCreator,
        OpenFiler<M, ChunkFiler> filerOpener,
        TxNamedMapOfFilerOverwriteGrowerProvider<H, M> overwriteGrowerProvider,
        TxNamedMapOfFilerRewriteGrowerProvider<H, M> rewriteGrowerProvider) {
        // TODO consider replacing with builder pattern
        this.name = name;
        this.namedMapOfFilers = lexOrderKeys
            ? new TxPartitionedNamedMapOfFiler<>(ByteArrayPartitionFunction.INSTANCE, createOrder(cogs, seed, chunkStores, filerCreator, filerOpener,
                    overwriteGrowerProvider, rewriteGrowerProvider))
            : new TxPartitionedNamedMapOfFiler<>(ByteArrayPartitionFunction.INSTANCE, create(cogs, seed, chunkStores, filerCreator, filerOpener,
                    overwriteGrowerProvider, rewriteGrowerProvider));

    }

    TxNamedMapOfFiler<SkipListMapBackedKeyedFPIndex, H, M>[] createOrder(TxCogs cogs,
        int seed,
        ChunkStore[] chunkStores,
        CreateFiler<H, M, ChunkFiler> filerCreator,
        OpenFiler<M, ChunkFiler> filerOpener,
        TxNamedMapOfFilerOverwriteGrowerProvider<H, M> overwriteGrowerProvider,
        TxNamedMapOfFilerRewriteGrowerProvider<H, M> rewriteGrowerProvider) {

        TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyhookCog = cogs.getSkyhookCog(seed);
        TxCog<Integer, SkipListMapBackedKeyedFPIndex, ChunkFiler> skipListPowerCog = cogs.getSkipListPowerCog(seed);

        @SuppressWarnings("unchecked")
        TxNamedMapOfFiler<SkipListMapBackedKeyedFPIndex, H, M>[] stores = new TxNamedMapOfFiler[chunkStores.length];
        for (int i = 0; i < stores.length; i++) {
            stores[i] = new TxNamedMapOfFiler<>(skyhookCog, seed, chunkStores[i], SKY_HOOK_FP,
                skipListPowerCog.creators,
                skipListPowerCog.opener,
                skipListPowerCog.grower,
                filerCreator,
                filerOpener,
                overwriteGrowerProvider,
                rewriteGrowerProvider);
        }
        return stores;
    }

    TxNamedMapOfFiler<MapBackedKeyedFPIndex, H, M>[] create(TxCogs cogs,
        int seed,
        ChunkStore[] chunkStores,
        CreateFiler<H, M, ChunkFiler> filerCreator,
        OpenFiler<M, ChunkFiler> filerOpener,
        TxNamedMapOfFilerOverwriteGrowerProvider<H, M> overwriteGrowerProvider,
        TxNamedMapOfFilerRewriteGrowerProvider<H, M> rewriteGrowerProvider) {

        TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyhookCog = cogs.getSkyhookCog(seed);
        TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> powerCog = cogs.getPowerCog(seed);

        @SuppressWarnings("unchecked")
        TxNamedMapOfFiler<MapBackedKeyedFPIndex, H, M>[] stores = new TxNamedMapOfFiler[chunkStores.length];
        for (int i = 0; i < stores.length; i++) {

            stores[i] = new TxNamedMapOfFiler<>(skyhookCog, seed, chunkStores[i], SKY_HOOK_FP,
                powerCog.creators,
                powerCog.opener,
                powerCog.grower,
                filerCreator,
                filerOpener,
                overwriteGrowerProvider,
                rewriteGrowerProvider);
        }
        return stores;
    }

    @Override
    public <R> R read(byte[] keyBytes, H newFilerInitialCapacity, final ChunkTransaction<M, R> transaction) throws IOException {
        return namedMapOfFilers.read(keyBytes, name, keyBytes, transaction);
    }

    @Override
    public <R> List<R> readEach(byte[][] eachKeyBytes, H newFilerInitialCapacity, ChunkTransaction<M, R> transaction) throws IOException {
        return namedMapOfFilers.readEach(eachKeyBytes, name, eachKeyBytes, transaction);
    }

    @Override
    public <R> R readWriteAutoGrow(byte[] keyBytes, H newFilerInitialCapacity, final ChunkTransaction<M, R> transaction) throws IOException {
        return namedMapOfFilers.readWriteAutoGrow(keyBytes, name, keyBytes, newFilerInitialCapacity, transaction);

    }

    @Override
    public <R> R writeNewReplace(byte[] keyBytes, H newFilerInitialCapacity, final ChunkTransaction<M, R> transaction) throws IOException {
        return namedMapOfFilers.writeNewReplace(keyBytes, name, keyBytes, newFilerInitialCapacity, transaction);
    }

    @Override
    public boolean stream(List<KeyRange> ranges, final KeyValueStore.EntryStream<IBA, Filer> stream) throws IOException {
        return namedMapOfFilers.stream(name, ranges, new TxStream<byte[], M, ChunkFiler>() {

            @Override
            public boolean stream(byte[] key, M monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    return stream.stream(new IBA(key), filer);
                }
            }
        });
    }

    @Override
    public boolean streamKeys(List<KeyRange> ranges, final KeyValueStore.KeyStream<IBA> stream) throws IOException {
        return namedMapOfFilers.streamKeys(name, ranges, new TxStreamKeys<byte[]>() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                if (key != null) {
                    return stream.stream(new IBA(key));
                }
                return true;
            }
        });
    }

    @Override
    public void close() {
    }

}
