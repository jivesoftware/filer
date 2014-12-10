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
package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.FilerTransaction;
import java.io.IOException;

/**
 * @author jonathan
 */
public interface MultiChunkStore {

    void allChunks(ChunkIdStream _chunks) throws IOException;

    long newChunk(byte[] key, long _capacity) throws IOException;

    <R> R execute(byte[] key, long _chunkFP, FilerTransaction<ChunkFiler, R> filerTransaction) throws IOException;

    ResizingChunkFilerProvider getChunkFilerProvider(byte[] key, ChunkFPProvider chunkFPProvider);

    ResizingChunkFilerProvider getTemporaryFilerProvider(byte[] keyBytes);

    void remove(byte[] key, long _chunkFP) throws IOException;

    /**
     * Destroys all the back chunk stores
     *
     * @throws IOException
     */
    void delete() throws IOException;

    interface ChunkFPProvider {
        long getChunkFP(byte[] key) throws IOException;

        void setChunkFP(byte[] key, long chunkFP) throws IOException;

        long getAndSetChunkFP(byte[] keyBytes, long newChunkFP) throws IOException;
    }

    /**
     *
     */
    interface ResizingChunkFilerProvider {

        Object lock();

        void init(long initialChunkSize) throws IOException;

        boolean open() throws IOException;

        ChunkFiler get() throws IOException;

        void transferTo(ResizingChunkFilerProvider to) throws IOException;

        void set(long newChunkFP) throws IOException;

        ChunkFiler grow(long capacity) throws IOException;
    }
}
