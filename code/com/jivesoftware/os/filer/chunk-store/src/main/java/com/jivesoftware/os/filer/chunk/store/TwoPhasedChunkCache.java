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
import java.util.concurrent.Semaphore;

/**
 *
 * @author jonathan.colt
 */
public class TwoPhasedChunkCache {

    private static final byte[] name = new byte[]{0};

    private final ByteBufferFactory bufferFactory;
    private  ChunkCache oldCache;
    private  ChunkCache newCache;
    private final int numPermits;
    private final Semaphore semaphore;
    private final int maxNewCacheSize;

    public TwoPhasedChunkCache(ByteBufferFactory bufferFactory,
        int numPermits,
        int maxNewCacheSize) {
        this.bufferFactory = bufferFactory;
        this.oldCache = new ChunkCache(name, bufferFactory);
        this.newCache = new ChunkCache(name, bufferFactory);
        this.numPermits = numPermits;
        this.semaphore = new Semaphore(numPermits, true);
        this.maxNewCacheSize = maxNewCacheSize;
    }

    public <M> void set(long chunkFP, long startOfFP, long endOfFP, M monkey) throws IOException {
        if (newCache.approxSize() > maxNewCacheSize) {
            // TODO add metrics around number of swaps and size of old cache
            try {
                semaphore.acquire(numPermits);
            } catch (InterruptedException ex) {
                throw new IOException("Failed to aquire semaphore during contains", ex);
            }
            try {
                oldCache = newCache;
                newCache = new ChunkCache(name, bufferFactory);
            } finally {
                semaphore.release(numPermits);
            }
        }

        try {
            semaphore.acquire();
        } catch (InterruptedException ex) {
            throw new IOException("Failed to aquire semaphore during contains", ex);
        }
        try {
            newCache.set(chunkFP, startOfFP, endOfFP, monkey);
            oldCache.remove(chunkFP);
        } finally {
            semaphore.release();
        }

    }

    public boolean contains(long chunkFP) throws IOException {
        try {
            semaphore.acquire();
        } catch (InterruptedException ex) {
            throw new IOException("Failed to aquire semaphore during contains", ex);
        }
        try {
            boolean had = newCache.contains(chunkFP);
            if (had == false) {
                return oldCache.contains(chunkFP);
            }
            return true;
        } finally {
            semaphore.release();
        }
    }

    public <M> Chunk<M> get(long chunkFP) throws IOException {
        try {
            semaphore.acquire();
        } catch (InterruptedException ex) {
            throw new IOException("Failed to aquire semaphore during removal", ex);
        }
        try {
            Chunk<M> chunk = newCache.get(chunkFP);
            if (chunk == null) {
                chunk = oldCache.get(chunkFP);
                if (chunk != null) {
                    newCache.set(chunkFP, chunk.startOfFP, chunk.endOfFP, chunk.monkey);
                    oldCache.remove(chunkFP);
                }
            }
            return chunk;

        } finally {
            semaphore.release();
        }
    }

    public void remove(long chunkFP) throws IOException {
        try {
            semaphore.acquire();
        } catch (InterruptedException ex) {
            throw new IOException("Failed to aquire semaphore during removal", ex);
        }
        try {
            oldCache.remove(chunkFP);
            newCache.remove(chunkFP);
        } finally {
            semaphore.release();
        }
    }

}
