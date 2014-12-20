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

import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.OpenFiler;
import java.io.IOException;
import java.util.Arrays;

/**
 * @param <F>
 * @author jonathan.colt
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

    private final CreateFiler<Long, MapContext, F> createFiler = new CreateFiler<Long, MapContext, F>() {
        @Override
        public MapContext create(Long hint, F filer) throws IOException {
            return mapStore.create(initialPageCapacity, keySize, variableKeySizes, payloadSize, variablePayloadSizes, filer);
        }

        @Override
        public long sizeInBytes(Long hint) throws IOException {
            return mapStore.computeFilerSize(initialPageCapacity, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
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
        final int newSize,
        final MapStore.CopyToStream copyToStream,
        final MapTransaction<F, R> chunkTransaction) throws IOException {

        long filerSize = mapStore.computeFilerSize(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
        return concurrentFilerProviders[getPageIndex(pageKey)].grow(filerSize,
            openFiler,
            new CreateFiler<Long, MapContext, F>() {
                @Override
                public MapContext create(Long hint, F filer) throws IOException {
                    return mapStore.create(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes, filer);
                }

                @Override
                public long sizeInBytes(Long hint) throws IOException {
                    return mapStore.computeFilerSize(newSize, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
                }
            },
            new RewriteMapChunkTransaction<F, R>() {
                @Override
                public R commit(MapContext currentContext, F currentFiler, MapContext newContext, F newFiler) throws IOException {
                    mapStore.copyTo(currentFiler, currentContext, newFiler, newContext, copyToStream);
                    return chunkTransaction.commit(newContext, newFiler);
                }
            });
    }

    @Override
    public <R> R copy(final byte[] pageKey, final MapContext fromContext, final F fromFiler, final MapTransaction<F, R> chunkTransaction) throws IOException {

        long filerSize = mapStore.computeFilerSize(fromContext.maxCount, keySize, variableKeySizes, payloadSize, variablePayloadSizes);
        return concurrentFilerProviders[getPageIndex(pageKey)].getOrAllocate(filerSize, openFiler, createFiler, new MapTransaction<F, R>() {
            @Override
            public R commit(MapContext newContext, F newFiler) throws IOException {
                mapStore.copyTo(fromFiler, fromContext, newFiler, newContext, null);
                return chunkTransaction.commit(newContext, newFiler);
            }
        });
    }

    @Override
    public boolean stream(final MapTransaction<F, Boolean> chunkTransaction) throws IOException {
        for (ConcurrentFilerProvider<F> concurrentFilerProvider : concurrentFilerProviders) {
            if (!concurrentFilerProvider.getOrAllocate(-1, openFiler, createFiler, chunkTransaction)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public <F2 extends Filer> void copyTo(MapChunkProvider<F2> toProvider) throws IOException {
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
