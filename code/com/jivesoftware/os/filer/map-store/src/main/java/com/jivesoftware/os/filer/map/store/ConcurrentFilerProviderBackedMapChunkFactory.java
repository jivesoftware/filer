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

/**
 * @author jonathan.colt
 * @param <F>
 */
public class ConcurrentFilerProviderBackedMapChunkFactory<F extends ConcurrentFiler> implements MapChunkFactory {

    private static final byte[] EMPTY_ID = new byte[16];

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
    public MapChunk getOrCreate(MapStore mapStore, String pageId) throws Exception {
        F filer = mapStore.allocateFiler(initialPageCapacity, keySize, variableKeySizes,
            payloadSize, variablePayloadSizes, concurrentFilerProvider);
        return mapStore.bootstrapAllocatedFiler(initialPageCapacity, keySize, variableKeySizes,
            payloadSize, variablePayloadSizes, filer);
    }

    @Override
    public MapChunk resize(MapStore mapStore, MapChunk chunk, String pageId, int newSize, MapStore.CopyToStream copyToStream) throws Exception {
        F filer = mapStore.allocateFiler(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes, concurrentFilerProvider);
        MapChunk newChunk = mapStore.bootstrapAllocatedFiler(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes, filer);
        mapStore.copyTo(chunk, newChunk, copyToStream);
        return newChunk;
    }

    @Override
    public MapChunk copy(MapStore mapStore, MapChunk chunk, String pageId, int newSize) throws Exception {
        return resize(mapStore, chunk, pageId, newSize, null);
    }

    @Override
    public MapChunk get(MapStore mapStore, String pageId) throws Exception {
        return null; // Since this impl doesn't persist there is nothing to get.
    }

    @Override
    public void delete(String pageId) throws Exception {
    }
}
