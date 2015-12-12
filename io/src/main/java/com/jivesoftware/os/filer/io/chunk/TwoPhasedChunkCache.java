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
package com.jivesoftware.os.filer.io.chunk;

import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkCache.CacheOpener;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class TwoPhasedChunkCache {

    private static final ChunkMetrics.ChunkMetric EVICTED = ChunkMetrics.get("TwoPhasedChunkCache", "chunkCache", "evicted");
    private static final ChunkMetrics.ChunkMetric EVICTIONS = ChunkMetrics.get("TwoPhasedChunkCache", "chunkCache", "evictions");
    private static final ChunkMetrics.ChunkMetric REVIVALS = ChunkMetrics.get("TwoPhasedChunkCache", "chunkCache", "revivals");
    private static final ChunkMetrics.ChunkMetric SLOUGHS_ALLOWED = ChunkMetrics.get("TwoPhasedChunkCache", "chunkCache", "sloughsAllowed");
    private static final ChunkMetrics.ChunkMetric SLOUGHS_REFUSED = ChunkMetrics.get("TwoPhasedChunkCache", "chunkCache", "sloughsRefused");
    private static final ChunkMetrics.ChunkMetric ROLLS_ALLOWED = ChunkMetrics.get("TwoPhasedChunkCache", "chunkCache", "rollsAllowed");
    private static final ChunkMetrics.ChunkMetric ROLLS_REFUSED = ChunkMetrics.get("TwoPhasedChunkCache", "chunkCache", "rollsRefused");

    private static final byte[] name = new byte[]{0};

    private final ByteBufferFactory bufferFactory;
    private ChunkCache oldCache;
    private ChunkCache newCache;
    private final int initialCacheSize;
    private final int maxNewCacheSize;

    public TwoPhasedChunkCache(ByteBufferFactory bufferFactory,
        int initialCacheSize,
        int maxNewCacheSize) {
        this.bufferFactory = bufferFactory;
        this.oldCache = new ChunkCache(name, bufferFactory);
        this.newCache = new ChunkCache(name, bufferFactory);
        this.initialCacheSize = initialCacheSize;
        this.maxNewCacheSize = maxNewCacheSize;
    }

    public <M> void set(long chunkFP, Chunk<M> chunk, StackBuffer stackBuffer) throws IOException {
        synchronized (this) {
            newCache.set(chunkFP, chunk, initialCacheSize, stackBuffer);
        }
    }

    public void roll() throws IOException {
        synchronized (this) {
            if (newCache.approxSize() == 0 && oldCache.approxSize() == 0) {
                // do nothing
            } else if (oldCache.isRemovable()) {
                evictAndAllocate();
                ROLLS_ALLOWED.inc(1);
            } else {
                ROLLS_REFUSED.inc(1);
            }
        }
    }

    public <M> Chunk<M> acquire(long chunkFP, final CacheOpener<M> opener, StackBuffer stackBuffer) throws IOException {
        boolean revived = false;
        try {
            synchronized (this) {
                if (newCache.approxSize() > maxNewCacheSize) {
                    if (oldCache.isRemovable()) {
                        evictAndAllocate();
                        SLOUGHS_ALLOWED.inc(1);
                    } else {
                        SLOUGHS_REFUSED.inc(1);
                    }
                }

                Chunk<M> chunk = newCache.acquireIfPresent(chunkFP, stackBuffer);
                if (chunk == null) {
                    chunk = oldCache.remove(chunkFP, stackBuffer);
                    if (chunk != null) {
                        chunk = newCache.promoteAndAcquire(chunkFP, chunk, initialCacheSize, stackBuffer);
                        revived = true;
                    } else {
                        chunk = newCache.promoteAndAcquire(chunkFP, opener.open(chunkFP), initialCacheSize, stackBuffer);
                    }
                }
                return chunk;
            }
        } finally {
            if (revived) {
                REVIVALS.inc(1);
            }
        }
    }

    /*
     * Synchronize externally.
     */
    private void evictAndAllocate() throws IOException {
        EVICTIONS.inc(1);
        EVICTED.inc((int) oldCache.approxSize());
        oldCache = newCache;
        newCache = new ChunkCache(name, bufferFactory);
    }

    public boolean contains(long chunkFP, StackBuffer stackBuffer) throws IOException {
        synchronized (this) {
            return newCache.contains(chunkFP, stackBuffer) || oldCache.contains(chunkFP, stackBuffer);
        }
    }

    public void release(long chunkFP, StackBuffer stackBuffer) throws IOException {
        synchronized (this) {
            if (!newCache.release(chunkFP, stackBuffer)) {
                if (!oldCache.release(chunkFP, stackBuffer)) {
                    throw new IllegalStateException("Attempted to release nonexistent chunkFP: " + chunkFP);
                }
            }
        }
    }

    public void remove(long chunkFP, StackBuffer stackBuffer) throws IOException {
        synchronized (this) {
            Chunk<Object> chunk = newCache.remove(chunkFP, stackBuffer);
            if (chunk == null) {
                chunk = oldCache.remove(chunkFP, stackBuffer);
                if (chunk == null) {
                    // probably rolled
                    return;
                }
            }
            if (chunk.acquisitions > 0) {
                throw new IllegalStateException("Removed an active chunkFP: " + chunkFP + " acquisitions=" + chunk.acquisitions);
            }
        }
    }
}
