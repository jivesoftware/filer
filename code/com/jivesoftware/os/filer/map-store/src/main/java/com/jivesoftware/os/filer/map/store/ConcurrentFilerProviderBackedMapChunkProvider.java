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
package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.OpenFiler;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author jonathan.colt
 * @param <F>
 */
public class ConcurrentFilerProviderBackedMapChunkProvider<F extends Filer> implements MapChunkProvider<F> {

    private static final MapStore mapStore = MapStore.INSTANCE;

    private final int keySize;
    private final boolean variableKeySizes;
    private final int payloadSize;
    private final boolean variablePayloadSizes;
    private final int initialPageCapacity;
    private final ConcurrentFilerProvider<F>[] concurrentFilerProviders;

    @SuppressWarnings("unchecked")
    public ConcurrentFilerProviderBackedMapChunkProvider(int keySize,
        boolean variableKeySizes,
        int payloadSize,
        boolean variablePayloadSizes,
        int initialPageCapacity,
        ConcurrentFilerProvider<F>[] concurrentFilerProviders) {
        this.keySize = keySize;
        this.variableKeySizes = variableKeySizes;
        this.payloadSize = payloadSize;
        this.variablePayloadSizes = variablePayloadSizes;
        this.initialPageCapacity = initialPageCapacity;
        this.concurrentFilerProviders = concurrentFilerProviders;
    }

    private final OpenFiler<MapContext, F> openFiler = new OpenFiler<MapContext, F>() {
        @Override
        public MapContext open(F filer) throws IOException {
            return mapStore.open(filer);
        }
    };

    private final CreateFiler<MapContext, F> createFiler = new CreateFiler<MapContext, F>() {
        @Override
        public MapContext create(F filer) throws IOException {
            return mapStore.create(initialPageCapacity, keySize, variableKeySizes, payloadSize, variablePayloadSizes, filer);
        }
    };

    @Override
    public <R> R get(byte[] pageKey, final MapTransaction<F, R> chunkTransaction) throws IOException {
        return concurrentFilerProviders[getPageIndex(pageKey)].getOrAllocate(-1, openFiler, createFiler, chunkTransaction);
    }

    @Override
    public <R> R getOrCreate(byte[] pageKey, final MapTransaction<F, R> chunkTransaction) throws IOException {
        int filerSize = mapStore.computeFilerSize(initialPageCapacity, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
        return concurrentFilerProviders[getPageIndex(pageKey)].getOrAllocate(filerSize, openFiler, createFiler, chunkTransaction);
    }

    @Override
    public <R> R grow(byte[] pageKey,
        final MapStore mapStore,
        final MapContext chunk,
        final int newSize,
        final MapStore.CopyToStream copyToStream,
        final MapTransaction<F, R> chunkTransaction) throws IOException {

        long filerSize = mapStore.computeFilerSize(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
        return concurrentFilerProviders[getPageIndex(pageKey)].grow(filerSize, new RewriteMapChunkTransaction<F, R>() {
            @Override
            public R commit(MapContext currentChunk, F currentFiler, MapContext newChunk, F newFiler) throws IOException {
                mapStore.copyTo(currentFiler, currentChunk, newFiler, newChunk, copyToStream);
                return chunkTransaction.commit(newChunk, newFiler);
            }
        });
    }

    @Override
    public <R> R copy(byte[] pageKey, MapStore mapStore, MapContext chunk, MapTransaction<F, R> chunkTransaction) throws IOException {
        return grow(pageKey, mapStore, chunk, chunk.maxCount, null, chunkTransaction);
    }

    @Override
    public void stream(final MapStore mapStore, final MapTransaction<F, Boolean> chunkTransaction) throws IOException {
        for (ConcurrentFilerProvider<F> concurrentFilerProvider : concurrentFilerProviders) {
            if (!concurrentFilerProvider.getOrAllocate(-1, openFiler, createFiler, chunkTransaction)) {
                break;
            }
        }
        chunkTransaction.commit(null, null);
    }

    @Override
    public <F2 extends ConcurrentFiler> void copyTo(final MapStore mapStore, MapChunkProvider<F2> toProvider) throws IOException {
        throw new UnsupportedOperationException("Out of scope");
    }

    @Override
    public void delete(byte[] pageKey) throws IOException {
        concurrentFilerProviders[getPageIndex(pageKey)].delete();
    }

    private int getPageIndex(byte[] pageKey) {
        return Math.abs(Arrays.hashCode(pageKey)) % concurrentFilerProviders.length;
    }
}
