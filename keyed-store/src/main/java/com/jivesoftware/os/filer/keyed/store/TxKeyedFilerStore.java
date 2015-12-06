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
import com.jivesoftware.os.filer.io.ByteArrayPartitionFunction;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.IndexAlignedChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import java.io.IOException;
import java.util.List;

/**
 * @param <H>
 * @param <M>
 * @author jonathan.colt
 */
public class TxKeyedFilerStore<H, M> implements KeyedFilerStore<H, M> {

    static final long SKY_HOOK_FP = 464; // I died a little bit doing this.

    private final byte[] name;
    private final TxPartitionedNamedMapOfFiler<?, H, M> namedMapOfFiler;

    public TxKeyedFilerStore(TxCogs cogs,
        int seed,
        ChunkStore[] chunkStores,
        byte[] name,
        boolean lexOrderKeys,
        CreateFiler<H, M, ChunkFiler> filerCreator,
        OpenFiler<M, ChunkFiler> filerOpener,
        TxNamedMapOfFilerOverwriteGrowerProvider<H, M> overwriteGrowerProvider,
        TxNamedMapOfFilerRewriteGrowerProvider<H, M> rewriteGrowerProvider) {
        // TODO consider replacing with builder pattern
        this.name = name;
        this.namedMapOfFiler = lexOrderKeys
            ? createOrder(cogs, seed, chunkStores, filerCreator, filerOpener, overwriteGrowerProvider, rewriteGrowerProvider)
            : create(cogs, seed, chunkStores, filerCreator, filerOpener, overwriteGrowerProvider, rewriteGrowerProvider);

    }

    TxPartitionedNamedMapOfFiler<SkipListMapBackedKeyedFPIndex, H, M> createOrder(TxCogs cogs,
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
                cogs.getSkyHookKeySemaphores(),
                cogs.getNamedKeySemaphores(),
                overwriteGrowerProvider,
                rewriteGrowerProvider);
        }
        return new TxPartitionedNamedMapOfFiler<>(ByteArrayPartitionFunction.INSTANCE, stores);
    }

    TxPartitionedNamedMapOfFiler<MapBackedKeyedFPIndex, H, M> create(TxCogs cogs,
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
                cogs.getSkyHookKeySemaphores(),
                cogs.getNamedKeySemaphores(),
                overwriteGrowerProvider,
                rewriteGrowerProvider);
        }
        return new TxPartitionedNamedMapOfFiler<>(ByteArrayPartitionFunction.INSTANCE, stores);
    }

    @Override
    public <R> R read(byte[] keyBytes,
        H newFilerInitialCapacity,
        ChunkTransaction<M, R> transaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        return namedMapOfFiler.read(keyBytes, name, keyBytes, transaction, stackBuffer);
    }

    @Override
    public <R> void readEach(byte[][] eachKeyBytes,
        H newFilerInitialCapacity,
        IndexAlignedChunkTransaction<M, R> transaction,
        R[] results,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        namedMapOfFiler.readEach(eachKeyBytes, name, eachKeyBytes, transaction, results, stackBuffer);
    }

    @Override
    public <R> R readWriteAutoGrow(byte[] keyBytes,
        H newFilerInitialCapacity,
        ChunkTransaction<M, R> transaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        return namedMapOfFiler.readWriteAutoGrow(keyBytes, name, keyBytes, newFilerInitialCapacity, transaction, stackBuffer);

    }

    @Override
    public <R> R writeNewReplace(byte[] keyBytes,
        H newFilerInitialCapacity,
        ChunkTransaction<M, R> transaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        return namedMapOfFiler.writeNewReplace(keyBytes, name, keyBytes, newFilerInitialCapacity, transaction, stackBuffer);
    }

    @Override
    public boolean stream(List<KeyRange> ranges,
        KeyValueStore.EntryStream<byte[], Filer> stream,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        return namedMapOfFiler.stream(name, ranges, (key, monkey, filer, lock) -> {
            synchronized (lock) {
                return stream.stream(key, filer);
            }
        }, stackBuffer);
    }

    @Override
    public boolean streamKeys(List<KeyRange> ranges,
        KeyValueStore.KeyStream<byte[]> stream,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        return namedMapOfFiler.streamKeys(name, ranges, key -> {
            if (key != null) {
                return stream.stream(key);
            }
            return true;
        }, stackBuffer);
    }

    @Override
    public long size(StackBuffer stackBuffer) throws IOException, InterruptedException {
        return namedMapOfFiler.size(name, stackBuffer);
    }

    @Override
    public void close() {
    }

}
