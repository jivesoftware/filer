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

import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
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
        ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {

        semaphore.acquire();

        try {
            long fp;
            synchronized (keyLock) {
                fp = backingFPIndex.get(key, stackBuffer);
                if (fp < 0) {
                    return filerTransaction.commit(null, null, stackBuffer, null);
                }
            }

            return chunkStore.execute(fp, opener, filerTransaction, stackBuffer);
        } finally {
            semaphore.release();
        }
    }

    public <H, K, M, R, G> R writeNewReplace(final FPIndex<K, ?> backingFPIndex,
        final Semaphore semaphore,
        final int numPermits,
        final ChunkStore chunkStore,
        final Object keyLock,
        final K key,
        H hint,
        final CreateFiler<H, M, ChunkFiler> creator,
        final OpenFiler<M, ChunkFiler> opener,
        final GrowFiler<H, M, ChunkFiler> growFiler,
        final ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {

        semaphore.acquire();

        final AtomicInteger releasablePermits = new AtomicInteger(1);
        try {

            final long grownFP = chunkStore.newChunk(hint, creator, stackBuffer);
            R result;
            final AtomicLong currentFP = new AtomicLong(-1);
            result = chunkStore.execute(grownFP, opener, (newMonkey, newFiler, _stackBuffer, newLock) -> {
                growFiler.growAndAcquire(hint, null, null, newMonkey, newFiler, newLock, newLock, _stackBuffer);
                try {
                    synchronized (keyLock) {
                        currentFP.set(backingFPIndex.getAndSet(key, grownFP, _stackBuffer));
                    }
                    return filerTransaction.commit(newMonkey, newFiler, _stackBuffer, newLock);
                } finally {
                    growFiler.release(hint, newMonkey, newLock);
                }
            }, stackBuffer);

            if (currentFP.get() > -1) {
                semaphore.release();
                releasablePermits.set(0);
                while (!semaphore.tryAcquire(numPermits, 5, TimeUnit.MINUTES)) {
                    System.err.println("Deadlock due to probable case of reentrant transaction");
                    Thread.dumpStack();
                }
                releasablePermits.set(numPermits);
                try {
                    chunkStore.remove(currentFP.get(), stackBuffer);
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

    public <H, K, M, R, G> R readWriteAutoGrowIfNeeded(final FPIndex<K, ?> backingFPIndex,
        final Semaphore semaphore,
        final int numPermits,
        final ChunkStore chunkStore,
        final Object keyLock,
        final K key,
        H hint,
        final CreateFiler<H, M, ChunkFiler> creator,
        final OpenFiler<M, ChunkFiler> opener,
        final GrowFiler<H, M, ChunkFiler> growFiler,
        final ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {

        long fp;

        semaphore.acquire();

        try {
            synchronized (keyLock) {
                fp = backingFPIndex.get(key, stackBuffer);
                if (fp < 0) {
                    if (creator == null) {
                        return filerTransaction.commit(null, null, stackBuffer, null);
                    }
                    final long newFp = chunkStore.newChunk(hint, creator, stackBuffer);
                    backingFPIndex.set(key, newFp, stackBuffer);
                    fp = newFp;
                }
            }

            Bag<R> bag = chunkStore.execute(fp, opener, (monkey, filer, _stackBuffer, lock) -> {
                if (growFiler != null) {
                    H hint1 = growFiler.acquire(hint, monkey, filer, lock);
                    try {
                        if (hint1 != null) {
                            return null;
                        }
                        return new Bag<>(filerTransaction.commit(monkey, filer, _stackBuffer, lock));
                    } finally {
                        growFiler.release(hint, monkey, lock);
                    }
                } else {
                    return new Bag<>(filerTransaction.commit(monkey, filer, _stackBuffer, lock));
                }
            }, stackBuffer);
            if (bag != null) {
                return bag.result;
            }
        } finally {
            semaphore.release();
        }

        while (!semaphore.tryAcquire(numPermits, 5, TimeUnit.MINUTES)) {
            System.err.println("Deadlock due to probable case of reentrant transaction");
            Thread.dumpStack();
        }

        final AtomicInteger releasablePermits = new AtomicInteger(numPermits);
        try {
            fp = backingFPIndex.get(key, stackBuffer);
            if (fp < 0) {
                throw new RuntimeException("Chunk disappeared!");
            }
            final AtomicLong removeFP = new AtomicLong(-1);
            try {
                return chunkStore.execute(fp, opener, (monkey, filer, stackBuffer1, lock) -> {
                    H hint1 = growFiler.acquire(hint, monkey, filer, lock);
                    try {
                        if (hint1 != null) {
                            final long grownFP = chunkStore.newChunk(hint1, creator, stackBuffer1);
                            return chunkStore.execute(grownFP, opener, (newMonkey, newFiler, stackBuffer2, newLock) -> {
                                growFiler.growAndAcquire(hint, monkey, filer, newMonkey, newFiler, lock, newLock, stackBuffer2);
                                try {
                                    backingFPIndex.set(key, grownFP, stackBuffer2);
                                    removeFP.set(filer.getChunkFP());

                                    semaphore.release(numPermits - 1);
                                    releasablePermits.set(1);
                                    return filerTransaction.commit(newMonkey, newFiler, stackBuffer2, newLock);
                                } finally {
                                    growFiler.release(hint, newMonkey, newLock);
                                }
                            }, stackBuffer1);

                        } else {
                            semaphore.release(numPermits - 1);
                            releasablePermits.set(1);

                            return filerTransaction.commit(monkey, filer, stackBuffer1, lock);
                        }
                    } finally {
                        growFiler.release(hint, monkey, lock);
                    }
                }, stackBuffer);
            } finally {
                if (removeFP.get() >= 0) {
                    chunkStore.remove(removeFP.get(), stackBuffer);
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
