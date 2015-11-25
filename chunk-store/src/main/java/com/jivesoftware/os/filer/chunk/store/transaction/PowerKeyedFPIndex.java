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
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import java.io.IOException;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class PowerKeyedFPIndex implements FPIndex<Integer, PowerKeyedFPIndex> {

    private final int seed;
    private final ChunkStore backingChunkStore;
    private final long backingFP;
    private final long[] fpIndex;
    private final IntIndexSemaphore keySemaphores;
    private final Object[] keySizeLocks;

    PowerKeyedFPIndex(int seed, ChunkStore chunkStore, long fp, long[] fpIndex, IntIndexSemaphore keySemaphores) throws IOException {

        this.seed = seed;
        this.backingChunkStore = chunkStore;
        this.backingFP = fp;
        this.fpIndex = fpIndex;
        this.keySemaphores = keySemaphores;
        this.keySizeLocks = new Object[fpIndex.length];
        for (int i = 0; i < fpIndex.length; i++) {
            keySizeLocks[i] = new Object();
        }
    }

    @Override
    public long get(Integer key, StackBuffer stackBuffer) throws IOException {
        return fpIndex[key];
    }

    @Override
    public void set(final Integer key, final long fp, StackBuffer stackBuffer) throws IOException, InterruptedException {
        backingChunkStore.execute(backingFP, null, (monkey, filer, _stackBuffer, lock) -> {
            synchronized (lock) {
                filer.seek(8 + (8 * key));
                FilerIO.writeLong(filer, fp, "fp", _stackBuffer);
            }
            return null;
        }, stackBuffer);
        fpIndex[key] = fp;
    }

    @Override
    public long getAndSet(final Integer key, final long fp, StackBuffer stackBuffer) throws IOException, InterruptedException {
        long got = fpIndex[key];
        backingChunkStore.execute(backingFP, null, (monkey, filer, _stackBuffer, lock) -> {
            synchronized (lock) {
                filer.seek(8 + (8 * key));
                FilerIO.writeLong(filer, fp, "fp", _stackBuffer);
            }
            return null;
        }, stackBuffer);
        fpIndex[key] = fp;
        return got;
    }

    @Override
    public <H, M, R> R read(ChunkStore chunkStore, Integer key,
        OpenFiler<M, ChunkFiler> opener, ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        return KeyedFPIndexUtil.INSTANCE.read(this, keySemaphores.semaphore(key, seed), keySemaphores.getNumPermits(), chunkStore, keySizeLocks[key],
            key, opener, filerTransaction, stackBuffer);
    }

    @Override
    public <H, M, R> R writeNewReplace(ChunkStore chunkStore, Integer key, H hint,
        CreateFiler<H, M, ChunkFiler> creator, OpenFiler<M, ChunkFiler> opener, GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        return KeyedFPIndexUtil.INSTANCE.writeNewReplace(this, keySemaphores.semaphore(key, seed), keySemaphores.getNumPermits(), chunkStore,
            keySizeLocks[key], key, hint, creator, opener, growFiler, filerTransaction, stackBuffer);
    }

    @Override
    public <H, M, R> R readWriteAutoGrow(ChunkStore chunkStore, Integer key, H hint,
        CreateFiler<H, M, ChunkFiler> creator, OpenFiler<M, ChunkFiler> opener, GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        return KeyedFPIndexUtil.INSTANCE.readWriteAutoGrowIfNeeded(this, keySemaphores.semaphore(key, seed), keySemaphores.getNumPermits(), chunkStore,
            keySizeLocks[key], key, hint, creator, opener, growFiler, filerTransaction, stackBuffer);
    }

    @Override
    public boolean stream(List<KeyRange> ranges, final KeysStream<Integer> keysStream, StackBuffer stackBuffer) throws IOException, InterruptedException {
        if (ranges != null) {
            throw new IllegalStateException("This doesn't make sense!");
        }
        for (int i = 0; i < fpIndex.length; i++) {
            if (fpIndex[i] > -1 && !keysStream.stream(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean acquire(int alwaysRoomForNMoreKeys) {
        return true;
    }

    @Override
    public int nextGrowSize(int alwaysRoomForNMoreKeys) throws IOException {
        return -1;
    }

    @Override
    public void copyTo(Filer curentFiler, FPIndex<Integer, PowerKeyedFPIndex> newMonkey, Filer newFiler, StackBuffer stackBuffer) throws IOException {
    }

    @Override
    public void release(int alwayRoomForNMoreKeys) {
    }

}
