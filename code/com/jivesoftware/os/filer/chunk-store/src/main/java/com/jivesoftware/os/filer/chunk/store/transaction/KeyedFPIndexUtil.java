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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class KeyedFPIndexUtil {

    public static KeyedFPIndexUtil INSTANCE = new KeyedFPIndexUtil();

    private KeyedFPIndexUtil() {
    }

    public <H, K, M, R> R read(FPIndex<K, ?> backingFPIndex,
        Semaphore semaphore,
        int numPermits,
        ChunkStore chunkStore,
        Object keyLock,
        K key,
        OpenFiler<M, ChunkFiler> opener,
        ChunkTransaction<M, R> filerTransaction) throws IOException {

        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to acquire 1 permits.", e);
        }

        try {
            long fp;
            synchronized (keyLock) {
                fp = backingFPIndex.get(key);
                if (fp < 0) {
                    return filerTransaction.commit(null, null, null);
                }
            }

            return chunkStore.execute(fp, opener, filerTransaction);
        } finally {
            semaphore.release();
        }
    }

    public <H, K, M, R> R writeNewReplace(final FPIndex<K, ?> backingFPIndex,
        final Semaphore semaphore,
        final int numPermits,
        final ChunkStore chunkStore,
        final Object keyLock,
        final K key,
        H hint,
        final CreateFiler<H, M, ChunkFiler> creator,
        final OpenFiler<M, ChunkFiler> opener,
        final GrowFiler<H, M, ChunkFiler> growFiler,
        final ChunkTransaction<M, R> filerTransaction) throws IOException {

        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to acquire 1 permits.", e);
        }

        final AtomicInteger releasablePermits = new AtomicInteger(1);
        try {

            final long grownFP = chunkStore.newChunk(hint, creator);
            R result;
            final AtomicLong currentFP = new AtomicLong(-1);
            result = chunkStore.execute(grownFP, opener, new ChunkTransaction<M, R>() {

                @Override
                public R commit(M newMonkey, ChunkFiler newFiler, Object newLock) throws IOException {
                    growFiler.growAndAcquire(null, null, newMonkey, newFiler, newLock, newLock);
                    try {
                        synchronized (keyLock) {
                            currentFP.set(backingFPIndex.getAndSet(key, grownFP));
                        }
                        return filerTransaction.commit(newMonkey, newFiler, newLock);
                    } finally {
                        growFiler.release(newMonkey, newLock);
                    }
                }
            });

            if (currentFP.get() > -1) {
                semaphore.release();
                releasablePermits.set(0);
                try {
                    while (!semaphore.tryAcquire(numPermits, 5, TimeUnit.MINUTES)) {
                        System.err.println("Deadlock due to probable case of reentrant transaction");
                        Thread.dumpStack();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed to acquire all permits.", e);
                }
                releasablePermits.set(numPermits);
                try {
                    chunkStore.remove(currentFP.get());
                } finally {
                    semaphore.release(numPermits - 1);
                    releasablePermits.set(1);
                }
            }
            return result;

        } finally {
            semaphore.release(releasablePermits.get());
        }
    }

    public <H, K, M, R> R readWriteAutoGrowIfNeeded(final FPIndex<K, ?> backingFPIndex,
        final Semaphore semaphore,
        final int numPermits,
        final ChunkStore chunkStore,
        final Object keyLock,
        final K key,
        H hint,
        final CreateFiler<H, M, ChunkFiler> creator,
        final OpenFiler<M, ChunkFiler> opener,
        final GrowFiler<H, M, ChunkFiler> growFiler,
        final ChunkTransaction<M, R> filerTransaction) throws IOException {

        long fp;

        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to acquire 1 permits.", e);
        }

        try {
            synchronized (keyLock) {
                fp = backingFPIndex.get(key);
                if (fp < 0) {
                    if (creator == null) {
                        return filerTransaction.commit(null, null, null);
                    }
                    final long newFp = chunkStore.newChunk(hint, creator);
                    backingFPIndex.set(key, newFp);
                    fp = newFp;
                }
            }

            Bag<R> bag = chunkStore.execute(fp, opener, new ChunkTransaction<M, Bag<R>>() {
                @Override
                public Bag<R> commit(M monkey, ChunkFiler filer, Object lock) throws IOException {
                    if (growFiler != null) {
                        H hint = growFiler.acquire(monkey, filer, lock);
                        try {
                            if (hint != null) {
                                return null;
                            }
                            return new Bag<>(filerTransaction.commit(monkey, filer, lock));
                        } finally {
                            growFiler.release(monkey, lock);
                        }
                    } else {
                        return new Bag<>(filerTransaction.commit(monkey, filer, lock));
                    }
                }
            });
            if (bag != null) {
                return bag.result;
            }
        } finally {
            semaphore.release();
        }

        try {
            while (!semaphore.tryAcquire(numPermits, 5, TimeUnit.MINUTES)) {
                System.err.println("Deadlock due to probable case of reentrant transaction");
                Thread.dumpStack();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to acquire all permits.", e);
        }
        final AtomicInteger releasablePermits = new AtomicInteger(numPermits);
        try {
            fp = backingFPIndex.get(key);
            if (fp < 0) {
                throw new RuntimeException("Chunk disappeared!");
            }
            final AtomicLong removeFP = new AtomicLong(-1);
            try {
                return chunkStore.execute(fp, opener, new ChunkTransaction<M, R>() {
                    @Override
                    public R commit(final M monkey, final ChunkFiler filer, final Object lock) throws IOException {
                        H hint = growFiler.acquire(monkey, filer, lock);
                        try {
                            if (hint != null) {
                                final long grownFP = chunkStore.newChunk(hint, creator);
                                return chunkStore.execute(grownFP, opener, new ChunkTransaction<M, R>() {

                                    @Override
                                    public R commit(M newMonkey, ChunkFiler newFiler, Object newLock) throws IOException {
                                        growFiler.growAndAcquire(monkey, filer, newMonkey, newFiler, lock, newLock);
                                        try {
                                            backingFPIndex.set(key, grownFP);
                                            removeFP.set(filer.getChunkFP());

                                            semaphore.release(numPermits - 1);
                                            releasablePermits.set(1);
                                            return filerTransaction.commit(newMonkey, newFiler, newLock);
                                        } finally {
                                            growFiler.release(newMonkey, newLock);
                                        }
                                    }
                                });

                            } else {
                                semaphore.release(numPermits - 1);
                                releasablePermits.set(1);

                                return filerTransaction.commit(monkey, filer, lock);
                            }
                        } finally {
                            growFiler.release(monkey, lock);
                        }
                    }
                });
            } finally {
                if (removeFP.get() >= 0) {
                    chunkStore.remove(removeFP.get());
                }
            }

        } finally {
            semaphore.release(releasablePermits.get());
        }
    }

    private static class Bag<R> {

        final R result;

        Bag(R result) {
            this.result = result;
        }

    }
}
