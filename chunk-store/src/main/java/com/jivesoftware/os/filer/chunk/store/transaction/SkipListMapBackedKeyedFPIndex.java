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
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.filer.io.map.SkipListMapContext;
import com.jivesoftware.os.filer.io.map.SkipListMapStore;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class SkipListMapBackedKeyedFPIndex implements FPIndex<byte[], SkipListMapBackedKeyedFPIndex> {

    private final int seed;
    private final ChunkStore backingChunkStore;
    private final long backingFP;
    private final SkipListMapContext context;
    private final SkipListMapBackedKeyedFPIndexOpener opener;
    private final LocksProvider<byte[]> keyLocks;
    private final SemaphoreProvider<byte[]> keySemaphores;
    private final Map<IBA, Long> keyToFpCache; // Nullable

    public SkipListMapBackedKeyedFPIndex(int seed,
        ChunkStore chunkStore,
        long fp,
        SkipListMapContext context,
        SkipListMapBackedKeyedFPIndexOpener opener,
        LocksProvider<byte[]> keyLocks,
        SemaphoreProvider<byte[]> keySemaphores,
        Map<IBA, Long> keyToFpCache) {

        this.seed = seed;
        this.backingChunkStore = chunkStore;
        this.backingFP = fp;
        this.context = context;
        this.opener = opener;
        this.keyLocks = keyLocks;
        this.keySemaphores = keySemaphores;
        this.keyToFpCache = keyToFpCache;
    }

    @Override
    public boolean acquire(int alwaysRoomForNMoreKeys) {
        return MapStore.INSTANCE.acquire(context.mapContext, alwaysRoomForNMoreKeys);
    }

    @Override
    public int nextGrowSize(int alwaysRoomForNMoreKeys) throws IOException {
        return MapStore.INSTANCE.nextGrowSize(context.mapContext, alwaysRoomForNMoreKeys);
    }

    @Override
    public void copyTo(Filer currentFiler, FPIndex<byte[], SkipListMapBackedKeyedFPIndex> newMonkey, Filer newFiler, byte[] primitiveBuffer) throws IOException {
        // TODO rework generics to elimnate this cast
        SkipListMapStore.INSTANCE.copyTo(currentFiler, context, newFiler, ((SkipListMapBackedKeyedFPIndex) newMonkey).context, null, primitiveBuffer);
    }

    @Override
    public void release(int alwayRoomForNMoreKeys) {
        MapStore.INSTANCE.release(context.mapContext, alwayRoomForNMoreKeys);
    }

    @Override
    public long get(final byte[] key, byte[] primitiveBuffer) throws IOException {
        Long got = null;
        IBA ibaKey = null;
        if (keyToFpCache != null) {
            ibaKey = new IBA(key);
            got = keyToFpCache.get(ibaKey);
        }
        if (got == null) {
            got = backingChunkStore.execute(backingFP, opener, (monkey, filer, _primitiveBuffer, lock) -> {
                synchronized (lock) {
                    byte[] got1 = SkipListMapStore.INSTANCE.getExistingPayload(filer, monkey.context, key, _primitiveBuffer);
                    if (got1 == null) {
                        return -1L;
                    }
                    return FilerIO.bytesLong(got1);
                }
            }, primitiveBuffer);
            if (keyToFpCache != null) {
                keyToFpCache.put(ibaKey, got);
            }
        }
        return got;
    }

    @Override
    public void set(final byte[] key, final long fp, byte[] primitiveBuffer) throws IOException {
        if (keyToFpCache != null) {
            keyToFpCache.put(new IBA(key), fp);
        }
        backingChunkStore.execute(backingFP, opener, (monkey, filer, _primitiveBuffer, lock) -> {
            synchronized (lock) {
                SkipListMapStore.INSTANCE.add(filer, context, key, FilerIO.longBytes(fp), _primitiveBuffer);
            }
            return null;
        }, primitiveBuffer);
    }

    @Override
    public long getAndSet(final byte[] key, final long fp, byte[] primitiveBuffer) throws IOException {
        if (keyToFpCache != null) {
            keyToFpCache.put(new IBA(key), fp);
        }
        return backingChunkStore.execute(backingFP, opener, (monkey, filer, _primitiveBuffer, lock) -> {
            synchronized (lock) {
                byte[] payload = SkipListMapStore.INSTANCE.getExistingPayload(filer, monkey.context, key, _primitiveBuffer);
                long got = -1L;
                if (payload != null) {
                    got = FilerIO.bytesLong(payload);
                }
                SkipListMapStore.INSTANCE.add(filer, context, key, FilerIO.longBytes(fp), _primitiveBuffer);
                return got;
            }
        }, primitiveBuffer);
    }

    @Override
    public <H, M, R> R read(ChunkStore chunkStore, byte[] key,
        OpenFiler<M, ChunkFiler> opener, ChunkTransaction<M, R> filerTransaction,
        byte[] primitiveBuffer) throws IOException {
        Object keyLock = keyLocks.lock(key, seed);
        return KeyedFPIndexUtil.INSTANCE.read(this, keySemaphores.semaphore(key, seed), keySemaphores.getNumPermits(), chunkStore, keyLock,
            key, opener, filerTransaction, primitiveBuffer);
    }

    @Override
    public <H, M, R> R writeNewReplace(ChunkStore chunkStore, byte[] key, H hint,
        CreateFiler<H, M, ChunkFiler> creator, OpenFiler<M, ChunkFiler> opener, GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction, byte[] primitiveBuffer) throws IOException {
        Object keyLock = keyLocks.lock(key, seed);
        return KeyedFPIndexUtil.INSTANCE.writeNewReplace(this, keySemaphores.semaphore(key, seed), keySemaphores.getNumPermits(), chunkStore, keyLock,
            key, hint, creator, opener, growFiler, filerTransaction, primitiveBuffer);
    }

    @Override
    public <H, M, R> R readWriteAutoGrow(ChunkStore chunkStore, byte[] key, H hint,
        CreateFiler<H, M, ChunkFiler> creator, OpenFiler<M, ChunkFiler> opener, GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction, byte[] primitiveBuffer) throws IOException {
        Object keyLock = keyLocks.lock(key, seed);
        return KeyedFPIndexUtil.INSTANCE.readWriteAutoGrowIfNeeded(this, keySemaphores.semaphore(key, seed), keySemaphores.getNumPermits(),
            chunkStore, keyLock, key, hint, creator, opener, growFiler, filerTransaction, primitiveBuffer);
    }

    @Override
    public boolean stream(final List<KeyRange> ranges, final KeysStream<byte[]> keysStream, byte[] primitiveBuffer) throws IOException {
        final MapStore.KeyStream mapKeyStream = keysStream::stream;

        return backingChunkStore.execute(backingFP, null, new ChunkTransaction<SkipListMapBackedKeyedFPIndex, Boolean>() {

            @Override
            public Boolean commit(SkipListMapBackedKeyedFPIndex monkey, ChunkFiler filer, byte[] primitiveBuffer, Object lock) throws IOException {
                return SkipListMapStore.INSTANCE.streamKeys(filer, monkey.context, lock, ranges, mapKeyStream, primitiveBuffer);
            }
        }, primitiveBuffer);
    }

}
