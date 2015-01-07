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
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author jonathan.colt
 */
public class KeyedFPIndexUtil {

    public static KeyedFPIndexUtil INSTANCE = new KeyedFPIndexUtil();

    private KeyedFPIndexUtil() {
    }

    public <H, M, K, R> R commit(final BackingFPIndex<K> backingFPIndex,
        SemaphoreAndCount semaphoreAndCount,
        final ChunkStore chunkStore,
        final Object keyLock,
        final K key,
        H hint,
        final CreateFiler<H, M, ChunkFiler> creator,
        final OpenFiler<M, ChunkFiler> opener,
        final GrowFiler<H, M, ChunkFiler> growFiler,
        final ChunkTransaction<M, R> filerTransaction) throws IOException {

        final Semaphore semaphore = semaphoreAndCount.semaphore;
        final int numPermits = semaphoreAndCount.numPermits;
        long fp;
        synchronized (keyLock) {
            fp = backingFPIndex.get(key);
            if (fp < 0) {
                if (creator == null) {
                    return filerTransaction.commit(null, null);
                }
                try {
                    semaphore.acquire(numPermits);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed to acquire all permits.", e);
                }
                try {
                    final long newFp = chunkStore.newChunk(hint, creator);
                    backingFPIndex.set(key, newFp);
                    fp = newFp;
                } finally {
                    semaphore.release(numPermits);
                }
            }
        }

        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to acquire 1 permits.", e);
        }

        try {
            Bag<R> bag = chunkStore.execute(fp, opener, new ChunkTransaction<M, Bag<R>>() {
                @Override
                public Bag<R> commit(M monkey, ChunkFiler filer) throws IOException {
                    if (growFiler != null) {
                        H hint = growFiler.grow(monkey, filer);
                        if (hint != null) {
                            return null;
                        }
                    }
                    return new Bag<>(filerTransaction.commit(monkey, filer));
                }
            });
            if (bag != null) {
                return bag.result;
            }
        } finally {
            semaphore.release();
        }

        synchronized (keyLock) {
            try {
                semaphore.acquire(numPermits);
            } catch (InterruptedException e) {
                throw new RuntimeException("Failed to acquire all permits.", e);
            }
            final AtomicInteger releasablePermits = new AtomicInteger(numPermits);
            try {
                return chunkStore.execute(fp, opener, new ChunkTransaction<M, R>() {
                    @Override
                    public R commit(final M monkey, final ChunkFiler filer) throws IOException {
                        H hint = growFiler.grow(monkey, filer);
                        if (hint != null) {
                            final long grownFP = chunkStore.newChunk(hint, creator);
                            chunkStore.execute(grownFP, opener, new ChunkTransaction<M, Void>() {

                                @Override
                                public Void commit(M newMonkey, ChunkFiler newFiler) throws IOException {
                                    growFiler.grow(monkey, filer, newMonkey, newFiler);
                                    return null;
                                }
                            });
                            backingFPIndex.set(key, grownFP);
                            chunkStore.remove(filer.getChunkFP());

                            semaphore.release(numPermits - 1);
                            releasablePermits.set(1);
                            return chunkStore.execute(grownFP, opener, new ChunkTransaction<M, R>() {
                                @Override
                                public R commit(M monkey, ChunkFiler filer) throws IOException {
                                    return filerTransaction.commit(monkey, filer);
                                }
                            });

                        } else {
                            semaphore.release(numPermits - 1);
                            releasablePermits.set(1);

                            return filerTransaction.commit(monkey, filer);
                        }
                    }
                });

            } finally {
                semaphore.release(releasablePermits.get());
            }
        }

    }

    public static interface BackingFPIndex<K> {

        long get(K key) throws IOException;

        void set(K key, long fp) throws IOException;
    }

    private static class Bag<R> {

        final R result;

        Bag(R result) {
            this.result = result;
        }

    }
}
