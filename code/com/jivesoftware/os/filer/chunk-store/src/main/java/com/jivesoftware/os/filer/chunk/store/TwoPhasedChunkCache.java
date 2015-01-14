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
 *
 * @author jonathan.colt
 */
public class TwoPhasedChunkCache {

    private static final byte[] name = new byte[]{0};

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
                // TODO add metrics around number of swaps and size of old cache
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
            if (had == false) {
                return oldCache.contains(chunkFP);
            }
            return true;
        }
    }

    public <M> Chunk<M> get(long chunkFP) throws IOException {
        synchronized (this) {
            Chunk<M> chunk = newCache.get(chunkFP);
            if (chunk == null) {
                chunk = oldCache.get(chunkFP);
                if (chunk != null) {
                    newCache.set(chunkFP, chunk.startOfFP, chunk.endOfFP, chunk.monkey);
                    oldCache.remove(chunkFP);
                }
            }
            return chunk;

        }
    }

    public void remove(long chunkFP) throws IOException {
        synchronized (this) {
            oldCache.remove(chunkFP);
            newCache.remove(chunkFP);
        }
    }
}
