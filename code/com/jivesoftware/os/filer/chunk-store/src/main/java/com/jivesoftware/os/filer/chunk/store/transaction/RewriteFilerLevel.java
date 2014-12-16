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
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class RewriteFilerLevel implements LevelProvider<KeyedFP<byte[]>, byte[], RewriteFilerLevel.RewriteFiler> {

    private final ChunkStore chunkStore;
    private final ByteArrayStripingLocksProvider locksProvider;
    private final long filerSize;

    public RewriteFilerLevel(ChunkStore chunkStore, ByteArrayStripingLocksProvider locksProvider, int filerSize) {
        this.chunkStore = chunkStore;
        this.locksProvider = locksProvider;
        this.filerSize = filerSize;
    }

    @Override
    public <R> R acquire(KeyedFP<byte[]> parentStore,
        byte[] levelKey,
        final StoreTransaction<R, RewriteFiler> storeTransaction) throws IOException {

        final long oldFP = parentStore.getFP(levelKey);
        final long newFp = chunkStore.newChunk(filerSize, null);
        try {
            final Object lock = locksProvider.lock(levelKey);
            synchronized (lock) {
                if (oldFP > 0) {
                    return chunkStore.execute(oldFP, null, new ChunkTransaction<Void, R>() {

                        @Override
                        public R commit(Void monkey, final ChunkFiler oldFiler) throws IOException {
                            return chunkStore.execute(newFp, null, new ChunkTransaction<Void, R>() {

                                @Override
                                public R commit(Void monkey, ChunkFiler newFiler) throws IOException {
                                    return storeTransaction.commit(new RewriteFiler(oldFiler, newFiler));
                                }
                            });
                        }
                    });
                } else {

                    return chunkStore.execute(newFp, null, new ChunkTransaction<Void, R>() {

                        @Override
                        public R commit(Void monkey, ChunkFiler newFiler) throws IOException {
                            return storeTransaction.commit(new RewriteFiler(null, newFiler));
                        }
                    });
                }
            }
        } catch (IOException x) {
            chunkStore.remove(newFp);
            throw x;
        }
    }

    @Override
    public void release(KeyedFP<byte[]> parentStore, byte[] levelKey, RewriteFiler rewriteFiler) throws IOException {
        long fp = parentStore.getFP(levelKey);
        parentStore.update(levelKey, rewriteFiler.newFiler.getChunkFP());
        chunkStore.remove(fp);
    }

    public static class RewriteFiler {

        public final ChunkFiler oldFiler;
        public final ChunkFiler newFiler;

        /**
         *
         * @param oldFiler nullable
         * @param newFiler
         */
        public RewriteFiler(ChunkFiler oldFiler, ChunkFiler newFiler) {
            this.oldFiler = oldFiler;
            this.newFiler = newFiler;
        }

    }

}
