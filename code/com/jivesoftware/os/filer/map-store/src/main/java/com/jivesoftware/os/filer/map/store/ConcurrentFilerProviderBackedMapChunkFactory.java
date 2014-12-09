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
import com.jivesoftware.os.filer.io.ConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import java.io.IOException;

/**
 * @author jonathan.colt
 * @param <F>
 */
public class ConcurrentFilerProviderBackedMapChunkFactory<F extends ConcurrentFiler> implements MapChunkFactory<F> {

    private final int keySize;
    private final boolean variableKeySizes;
    private final int payloadSize;
    private final boolean variablePayloadSizes;
    private final int initialPageCapacity;
    private final ConcurrentFilerProvider<F> concurrentFilerProvider;

    public ConcurrentFilerProviderBackedMapChunkFactory(int keySize,
        boolean variableKeySizes,
        int payloadSize,
        boolean variablePayloadSizes,
        int initialPageCapacity,
        ConcurrentFilerProvider<F> concurrentFilerProvider) {
        this.keySize = keySize;
        this.variableKeySizes = variableKeySizes;
        this.payloadSize = payloadSize;
        this.variablePayloadSizes = variablePayloadSizes;
        this.initialPageCapacity = initialPageCapacity;
        this.concurrentFilerProvider = concurrentFilerProvider;
    }

    @Override
    public MapChunk<F> getOrCreate(MapStore mapStore, String pageId) throws Exception {
        int filerSize = mapStore.computeFilerSize(initialPageCapacity, keySize, variableKeySizes,
            payloadSize, variablePayloadSizes);
        F filer = concurrentFilerProvider.allocate(filerSize);
        return mapStore.bootstrapAllocatedFiler(initialPageCapacity, keySize, variableKeySizes,
            payloadSize, variablePayloadSizes, filer);
    }

    @Override
    public MapChunk<F> resize(final MapStore mapStore, final MapChunk<F> chunk, String pageId, final int newSize, final MapStore.CopyToStream copyToStream)
        throws Exception {

        int filerSize = mapStore.computeFilerSize(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
        return concurrentFilerProvider.reallocate(filerSize, new ConcurrentFilerFactory.ReallocateFiler<F, MapChunk<F>>() {
            @Override
            public MapChunk<F> reallocate(F newFiler) throws IOException {
                MapChunk<F> newChunk = mapStore.bootstrapAllocatedFiler(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes, newFiler);
                mapStore.copyTo(chunk, newChunk, copyToStream);
                return newChunk;
            }
        });
    }

    @Override
    public MapChunk<F> copy(MapStore mapStore, MapChunk<F> chunk, String pageId, int newSize) throws Exception {
        return resize(mapStore, chunk, pageId, newSize, null);
    }

    @Override
    public MapChunk<F> get(MapStore mapStore, String pageId) throws Exception {
        F filer = concurrentFilerProvider.get();
        if (filer != null) {
            MapChunk<F> mapChunk = new MapChunk<>(filer);
            mapChunk.init(MapStore.DEFAULT);
            return mapChunk;
        }
        return null;
    }

    @Override
    public void delete(String pageId) throws Exception {
    }
}
