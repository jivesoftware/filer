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

import com.jivesoftware.os.filer.io.ByteBufferFactory;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class TwoPhasedChunkCache {

    private static final ChunkMetrics.ChunkMetric EVICTED = ChunkMetrics.get("chunkCache", "evicted");
    private static final ChunkMetrics.ChunkMetric EVICTIONS = ChunkMetrics.get("chunkCache", "evictions");
    private static final ChunkMetrics.ChunkMetric REVIVALS = ChunkMetrics.get("chunkCache", "revivals");

    private static final byte[] name = new byte[] { 0 };

    private final ByteBufferFactory bufferFactory;
    private ChunkCache oldCache;
    private ChunkCache newCache;
    private final int maxNewCacheSize;

    public TwoPhasedChunkCache(ByteBufferFactory bufferFactory,
        int maxNewCacheSize) {
        this.bufferFactory = bufferFactory;
        this.oldCache = new ChunkCache(name, bufferFactory);
        this.newCache = new ChunkCache(name, bufferFactory);
        this.maxNewCacheSize = maxNewCacheSize;
    }

    public <M> void set(long chunkFP, long startOfFP, long endOfFP, M monkey) throws IOException {
        synchronized (this) {
            if (newCache.approxSize() > maxNewCacheSize) {
                EVICTIONS.inc(1);
                EVICTED.inc((int) oldCache.approxSize());
                oldCache = newCache;
                newCache = new ChunkCache(name, bufferFactory);
            }

            newCache.set(chunkFP, startOfFP, endOfFP, monkey);
            oldCache.remove(chunkFP);
        }
    }

    public boolean contains(long chunkFP) throws IOException {
        synchronized (this) {
            boolean had = newCache.contains(chunkFP);
            if (!had) {
                return oldCache.contains(chunkFP);
            }
            return true;
        }
    }

    public <M> Chunk<M> get(long chunkFP) throws IOException {
        boolean revived = false;
        try {
            synchronized (this) {
                Chunk<M> chunk = newCache.get(chunkFP);
                if (chunk == null) {
                    chunk = oldCache.get(chunkFP);
                    if (chunk != null) {
                        newCache.set(chunkFP, chunk.startOfFP, chunk.endOfFP, chunk.monkey);
                        oldCache.remove(chunkFP);
                        revived = true;
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

    public void remove(long chunkFP) throws IOException {
        synchronized (this) {
            oldCache.remove(chunkFP);
            newCache.remove(chunkFP);
        }
    }
}
