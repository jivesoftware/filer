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
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.LocksProvider;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class MapBackedKeyedFPIndex implements FPIndex<byte[], MapBackedKeyedFPIndex> {

    private final int seed;
    private final ChunkStore backingChunkStore;
    private final long backingFP;
    private final MapContext mapContext;
    private final MapBackedKeyedFPIndexOpener opener;
    private final LocksProvider<byte[]> keyLocks;
    private final SemaphoreProvider<byte[]> keySemaphores;
    private final Map<IBA, Long> keyToFpCache; // Nullable

    public MapBackedKeyedFPIndex(int seed,
        ChunkStore chunkStore,
        long fp,
        MapContext mapContext,
        MapBackedKeyedFPIndexOpener opener,
        LocksProvider<byte[]> keyLocks,
        SemaphoreProvider<byte[]> keySemaphores,
        Map<IBA, Long> keyToFpCache) {

        this.seed = seed;
        this.backingChunkStore = chunkStore;
        this.backingFP = fp;
        this.mapContext = mapContext;
        this.opener = opener;
        this.keyLocks = keyLocks;
        this.keySemaphores = keySemaphores;
        this.keyToFpCache = keyToFpCache;
    }

    @Override
    public boolean acquire(int alwaysRoomForNMoreKeys) {
        return MapStore.INSTANCE.acquire(mapContext, alwaysRoomForNMoreKeys);
    }

    @Override
    public int nextGrowSize(int alwaysRoomForNMoreKeys) throws IOException {
        return MapStore.INSTANCE.nextGrowSize(mapContext, alwaysRoomForNMoreKeys);
    }

    @Override
    public void copyTo(Filer currentFiler, FPIndex<byte[], MapBackedKeyedFPIndex> newMonkey, Filer newFiler, StackBuffer stackBuffer) throws IOException {
        // TODO rework generics to elimnate this cast
        MapStore.INSTANCE.copyTo(currentFiler, mapContext, newFiler, ((MapBackedKeyedFPIndex) newMonkey).mapContext, null, stackBuffer);
    }

    @Override
    public void release(int alwayRoomForNMoreKeys) {
        MapStore.INSTANCE.release(mapContext, alwayRoomForNMoreKeys);
    }

    @Override
    public long get(final byte[] key, StackBuffer stackBuffer) throws IOException, InterruptedException {
        Long got = null;
        if (keyToFpCache != null) {
            got = keyToFpCache.get(stackBuffer.accessKey(key));
        }
        if (got == null) {
            got = backingChunkStore.execute(backingFP, opener, (monkey, filer, stackBuffer1, lock) -> {
                synchronized (lock) {
                    long ai = MapStore.INSTANCE.get(filer, monkey.mapContext, key, stackBuffer1);
                    if (ai < 0) {
                        return -1L;
                    }
                    return FilerIO.bytesLong(MapStore.INSTANCE.getPayload(filer, monkey.mapContext, ai, stackBuffer1));
                }
            }, stackBuffer);
            if (keyToFpCache != null) {
                keyToFpCache.put(new IBA(key), got);
            }
        }
        return got;
    }

    @Override
    public void set(final byte[] key, final long fp, StackBuffer stackBuffer) throws IOException, InterruptedException {
        if (keyToFpCache != null) {
            keyToFpCache.put(new IBA(key), fp);
        }
        backingChunkStore.execute(backingFP, opener, (monkey, filer, stackBuffer1, lock) -> {
            synchronized (lock) {
                MapStore.INSTANCE.add(filer, monkey.mapContext, (byte) 1, key, FilerIO.longBytes(fp), stackBuffer1);
            }
            return null;
        }, stackBuffer);
    }

    @Override
    public long getAndSet(final byte[] key, final long fp, StackBuffer stackBuffer) throws IOException, InterruptedException {
        if (keyToFpCache != null) {
            keyToFpCache.put(new IBA(key), fp);
        }
        return backingChunkStore.execute(backingFP, opener, (monkey, filer, stackBuffer1, lock) -> {
            synchronized (lock) {
                long ai = MapStore.INSTANCE.get(filer, monkey.mapContext, key, stackBuffer1);
                long got = -1L;
                if (ai > -1) {
                    got = FilerIO.bytesLong(MapStore.INSTANCE.getPayload(filer, monkey.mapContext, ai, stackBuffer1));
                }
                MapStore.INSTANCE.add(filer, monkey.mapContext, (byte) 1, key, FilerIO.longBytes(fp), stackBuffer1);
                return got;
            }
        }, stackBuffer);
    }

    @Override
    public <H, M, R> R read(ChunkStore chunkStore, byte[] key,
        OpenFiler<M, ChunkFiler> opener, ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        Object keyLock = keyLocks.lock(key, seed);
        return KeyedFPIndexUtil.INSTANCE.read(this, keySemaphores.semaphore(key, seed), keySemaphores.getNumPermits(), chunkStore, keyLock,
            key, opener, filerTransaction, stackBuffer);
    }

    @Override
    public <H, M, R> R writeNewReplace(ChunkStore chunkStore, byte[] key, H hint,
        CreateFiler<H, M, ChunkFiler> creator, OpenFiler<M, ChunkFiler> opener, GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        Object keyLock = keyLocks.lock(key, seed);
        return KeyedFPIndexUtil.INSTANCE.writeNewReplace(this, keySemaphores.semaphore(key, seed), keySemaphores.getNumPermits(), chunkStore, keyLock,
            key, hint, creator, opener, growFiler, filerTransaction, stackBuffer);
    }

    @Override
    public <H, M, R> R readWriteAutoGrow(ChunkStore chunkStore, byte[] key, H hint,
        CreateFiler<H, M, ChunkFiler> creator, OpenFiler<M, ChunkFiler> opener, GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction,
        StackBuffer stackBuffer) throws IOException, InterruptedException {
        Object keyLock = keyLocks.lock(key, seed);
        return KeyedFPIndexUtil.INSTANCE.readWriteAutoGrowIfNeeded(this, keySemaphores.semaphore(key, seed), keySemaphores.getNumPermits(),
            chunkStore, keyLock, key, hint, creator, opener, growFiler, filerTransaction, stackBuffer);
    }

    @Override
    public boolean stream(final List<KeyRange> ranges, final KeysStream<byte[]> keysStream, StackBuffer stackBuffer) throws IOException, InterruptedException {
        final MapStore.KeyStream mapKeyStream = key -> {
            if (ranges != null) {
                for (KeyRange range : ranges) {
                    if (range.contains(key)) {
                        if (!keysStream.stream(key)) {
                            return false;
                        }
                    }
                }
                return true;
            } else {
                return keysStream.stream(key);
            }
        };

        return backingChunkStore.execute(backingFP, null,
            (MapBackedKeyedFPIndex monkey, ChunkFiler filer, StackBuffer _stackBuffer, Object lock) -> MapStore.INSTANCE.streamKeys(filer,
                monkey.mapContext, lock, mapKeyStream, _stackBuffer), stackBuffer);
    }

    @Override
    public long size(StackBuffer stackBuffer) throws IOException, InterruptedException {
        return backingChunkStore.execute(backingFP, null,
            (MapBackedKeyedFPIndex monkey, ChunkFiler filer, StackBuffer _stackBuffer, Object lock) -> MapStore.INSTANCE.getCount(filer, _stackBuffer),
            stackBuffer);
    }
}
