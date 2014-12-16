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
package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkTransaction;
import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class ReadOnlyFilerLevel implements LevelProvider<KeyedFP<byte[]>, byte[], Filer> {

    private final ChunkStore chunkStore;
    private final ByteArrayStripingLocksProvider locksProvider;

    public ReadOnlyFilerLevel(ChunkStore chunkStore, ByteArrayStripingLocksProvider locksProvider) {
        this.chunkStore = chunkStore;
        this.locksProvider = locksProvider;
    }

    @Override
    public <R> R acquire(KeyedFP<byte[]> parentStore,
        byte[] levelKey,
        final StoreTransaction<R, Filer> storeTransaction) throws IOException {

        long fp = parentStore.getFP(levelKey);
        if (fp < 0) {
            return null;
        }
        final Object lock = locksProvider.lock(levelKey);
        synchronized (lock) {
            return chunkStore.execute(fp, null, new ChunkTransaction<Void, R>() {

                @Override
                public R commit(Void monkey, ChunkFiler filer) throws IOException {
                    return storeTransaction.commit(filer);
                }
            });
        }
    }

    @Override
    public void release(KeyedFP<byte[]> parentStore, byte[] levelKey, Filer filer) throws IOException {
    }

}
