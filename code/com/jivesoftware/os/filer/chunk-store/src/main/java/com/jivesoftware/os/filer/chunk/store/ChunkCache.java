/*
 * Copyright 2015 Jive Software.
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
package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class ChunkCache {

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final byte[] name;
    private final ByteBufferFactory bufferFactory;

    private MapContext mapContext;
    private ByteBufferBackedFiler mapFiler;
    private Chunk<?>[] chunks;
    private long acquisitions;

    public ChunkCache(byte[] name, ByteBufferFactory bufferFactory) {
        this.name = name;
        this.bufferFactory = bufferFactory;
    }

    public long approxSize() throws IOException {
        MapContext _mapContext = mapContext;
        if (_mapContext == null) {
            return 0;
        }
        return MapStore.INSTANCE.getApproxCount(mapContext);
    }

    <M> void set(long chunkFP, Chunk<M> chunk) throws IOException {
        ensureCapacity();
        long ai = MapStore.INSTANCE.add(mapFiler, mapContext, (byte) 1, FilerIO.longBytes(chunkFP), EMPTY_PAYLOAD);
        chunks[(int) ai] = chunk;
    }

    public boolean contains(long chunkFP) throws IOException {
        ensureCapacity();
        long ai = MapStore.INSTANCE.get(mapFiler, mapContext, FilerIO.longBytes(chunkFP));
        return ai > -1;
    }

    public <M> Chunk<M> acquireIfPresent(long chunkFP) throws IOException {
        ensureCapacity();
        long ai = MapStore.INSTANCE.get(mapFiler, mapContext, FilerIO.longBytes(chunkFP));
        if (ai > -1) {
            Chunk<M> chunk = (Chunk<M>) chunks[(int) ai];
            chunk.acquisitions++;
            acquisitions++;
            return chunk;
        } else {
            return null;
        }
    }

    public boolean release(long chunkFP) throws IOException {
        ensureCapacity();
        long ai = MapStore.INSTANCE.get(mapFiler, mapContext, FilerIO.longBytes(chunkFP));
        if (ai > -1) {
            chunks[(int) ai].acquisitions--;
            acquisitions--;
            return true;
        } else {
            return false;
        }
    }

    public <M> Chunk<M> remove(long chunkFP) throws IOException {
        ensureCapacity();
        long ai = MapStore.INSTANCE.remove(mapFiler, mapContext, FilerIO.longBytes(chunkFP));
        if (ai > -1) {
            Chunk<M> chunk = (Chunk<M>) chunks[(int) ai];
            acquisitions -= chunk.acquisitions;
            chunks[(int) ai] = null;
            return chunk;
        } else {
            return null;
        }
    }

    <M> Chunk<M> promoteAndAcquire(long chunkFP, Chunk<M> chunk) throws IOException {
        ensureCapacity();
        long ai = MapStore.INSTANCE.add(mapFiler, mapContext, (byte) 1, FilerIO.longBytes(chunkFP), EMPTY_PAYLOAD);
        if (ai == -1) {
            throw new IllegalStateException("Context has no room");
        }
        chunks[(int) ai] = chunk;
        chunk.acquisitions++;
        acquisitions += chunk.acquisitions;
        return chunk;
    }

    public boolean isRemovable() {
        return acquisitions == 0;
    }

    void ensureCapacity() throws IOException {
        if (mapContext == null) {
            int size = MapStore.INSTANCE.computeFilerSize(2, 8, false, 0, false);
            mapFiler = new ByteBufferBackedFiler(bufferFactory.allocate(name, size));
            mapContext = MapStore.INSTANCE.create(2, 8, false, 0, false, mapFiler);
            chunks = new Chunk[mapContext.capacity];
        } else {
            if (MapStore.INSTANCE.isFull(mapContext)) {
                int nextGrowSize = MapStore.INSTANCE.nextGrowSize(mapContext);
                int newSize = MapStore.INSTANCE.computeFilerSize(nextGrowSize, 8, false, 0, false);
                ByteBufferBackedFiler newMapFiler = new ByteBufferBackedFiler(bufferFactory.allocate(name, newSize));
                MapContext newMapContext = MapStore.INSTANCE.create(nextGrowSize, 8, false, 0, false, newMapFiler);
                final Chunk<?>[] newChunks = new Chunk[newMapContext.capacity];
                MapStore.INSTANCE.copyTo(mapFiler, mapContext, newMapFiler, newMapContext, new MapStore.CopyToStream() {
                    @Override
                    public void copied(long fromIndex, long toIndex) {
                        newChunks[(int) toIndex] = chunks[(int) fromIndex];
                    }
                });
                mapFiler = newMapFiler;
                mapContext = newMapContext;
                chunks = newChunks;

            }
        }
    }

    public interface CacheOpener<M> {

        Chunk<M> open(long chunkFP) throws IOException;
    }

}
