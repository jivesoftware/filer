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

import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author jonathan
 */
public class MultiChunkStore {

    final ChunkStore[] chunkStores;

    private final ByteArrayStripingLocksProvider[] locksProviders;

    public MultiChunkStore(ChunkStore... chunkStores) {
        this.chunkStores = chunkStores;
        this.locksProviders = new ByteArrayStripingLocksProvider[chunkStores.length];
        for (int i = 0; i < locksProviders.length; i++) {
            //TODO expose concurrency level
            locksProviders[i] = new ByteArrayStripingLocksProvider(1024);
        }
    }

    public void allChunks(ChunkIdStream _chunks) throws Exception {
        for (ChunkStore chunkStore : chunkStores) {
            chunkStore.allChunks(_chunks);
        }
    }

    public long newChunk(byte[] key, long _capacity) throws Exception {
        return chunkStores[Math.abs(Arrays.hashCode(key)) % chunkStores.length].newChunk(_capacity);
    }

    public Filer getFiler(byte[] key, long _chunkFP) throws Exception {
        int chunkIndex = Math.abs(Arrays.hashCode(key)) % chunkStores.length;
        Object lock = locksProviders[chunkIndex].lock(key);
        return chunkStores[chunkIndex].getFiler(_chunkFP, lock);
    }

    public void remove(byte[] key, long _chunkFP) throws Exception {
        chunkStores[Math.abs(Arrays.hashCode(key)) % chunkStores.length].remove(_chunkFP);
    }

    public void delete() throws Exception {
        for (ChunkStore chunkStore : chunkStores) {
            chunkStore.delete();
        }
    }

    public long sizeInBytes() throws IOException {
        long sizeInBytes = 0;
        for (ChunkStore chunkStore : chunkStores) {
            sizeInBytes += chunkStore.sizeInBytes();
        }
        return sizeInBytes;
    }

}
