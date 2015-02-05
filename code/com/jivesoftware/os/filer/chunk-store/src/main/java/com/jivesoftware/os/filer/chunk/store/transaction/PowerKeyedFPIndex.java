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
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.map.store.api.KeyRange;
import java.io.IOException;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class PowerKeyedFPIndex implements FPIndex<Integer, PowerKeyedFPIndex> {

    private final ChunkStore backingChunkStore;
    private final long backingFP;
    private final long[] fpIndex;
    private final IntIndexSemaphore keySemaphores = new IntIndexSemaphore(64, 64); //TODO expose?
    private final Object[] keySizeLocks;

    PowerKeyedFPIndex(ChunkStore chunkStore, long fp, long[] fpIndex) throws IOException {

        this.backingChunkStore = chunkStore;
        this.backingFP = fp;
        this.fpIndex = fpIndex;
        this.keySizeLocks = new Object[fpIndex.length];
        for (int i = 0; i < fpIndex.length; i++) {
            keySizeLocks[i] = new Object();
        }
    }

    @Override
    public long get(Integer key) throws IOException {
        return fpIndex[key];
    }

    @Override
    public void set(final Integer key, final long fp) throws IOException {
        backingChunkStore.execute(backingFP, null, new ChunkTransaction<PowerKeyedFPIndex, Void>() {

            @Override
            public Void commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    filer.seek(8 + (8 * key));
                    FilerIO.writeLong(filer, fp, "fp");
                }
                return null;
            }
        });
        fpIndex[key] = fp;
    }

    @Override
    public long getAndSet(final Integer key, final long fp) throws IOException {
        long got = fpIndex[key];
        backingChunkStore.execute(backingFP, null, new ChunkTransaction<PowerKeyedFPIndex, Void>() {

            @Override
            public Void commit(PowerKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    filer.seek(8 + (8 * key));
                    FilerIO.writeLong(filer, fp, "fp");
                }
                return null;
            }
        });
        fpIndex[key] = fp;
        return got;
    }

    @Override
    public <H, M, R> R read(ChunkStore chunkStore, Integer key,
        OpenFiler<M, ChunkFiler> opener, ChunkTransaction<M, R> filerTransaction) throws IOException {
        return KeyedFPIndexUtil.INSTANCE.read(this, keySemaphores.semaphore(key), keySemaphores.getNumPermits(), chunkStore, keySizeLocks[key],
            key, opener, filerTransaction);
    }

    @Override
    public <H, M, R> R writeNewReplace(ChunkStore chunkStore, Integer key, H hint,
        CreateFiler<H, M, ChunkFiler> creator, OpenFiler<M, ChunkFiler> opener, GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction) throws IOException {
        return KeyedFPIndexUtil.INSTANCE.writeNewReplace(this, keySemaphores.semaphore(key), keySemaphores.getNumPermits(), chunkStore,
            keySizeLocks[key], key, hint, creator, opener, growFiler, filerTransaction);
    }

    @Override
    public <H, M, R> R readWriteAutoGrow(ChunkStore chunkStore, Integer key, H hint,
        CreateFiler<H, M, ChunkFiler> creator, OpenFiler<M, ChunkFiler> opener, GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction) throws IOException {
        return KeyedFPIndexUtil.INSTANCE.readWriteAutoGrowIfNeeded(this, keySemaphores.semaphore(key), keySemaphores.getNumPermits(), chunkStore,
            keySizeLocks[key], key, hint, creator, opener, growFiler, filerTransaction);
    }

    @Override
    public boolean stream(List<KeyRange> ranges, final KeysStream<Integer> keysStream) throws IOException {
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
    public void copyTo(Filer curentFiler, FPIndex<Integer, PowerKeyedFPIndex> newMonkey, Filer newFiler) throws IOException {
    }

    @Override
    public void release(int alwayRoomForNMoreKeys) {
    }

}
