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
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.map.store.MapStore;
import com.jivesoftware.os.filer.map.store.SkipListMapContext;
import com.jivesoftware.os.filer.map.store.SkipListMapStore;
import com.jivesoftware.os.filer.map.store.api.KeyRange;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * @author jonathan.colt
 */
public class SkipListMapBackedKeyedFPIndex implements FPIndex<byte[], SkipListMapBackedKeyedFPIndex> {

    private final ChunkStore backingChunkStore;
    private final long backingFP;
    private final int numPermits = 64; // TODO expose ?
    private final Semaphore semaphore = new Semaphore(numPermits, true);
    private final SkipListMapContext context;
    private final SkipListMapBackedKeyedFPIndexOpener opener;
    private final ByteArrayStripingLocksProvider keyLocks;

    public SkipListMapBackedKeyedFPIndex(ChunkStore chunkStore,
        long fp,
        SkipListMapContext context,
        SkipListMapBackedKeyedFPIndexOpener opener,
        ByteArrayStripingLocksProvider keyLocks) {

        this.backingChunkStore = chunkStore;
        this.backingFP = fp;
        this.context = context;
        this.opener = opener;
        this.keyLocks = keyLocks;
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
    public void copyTo(Filer currentFiler, FPIndex<byte[], SkipListMapBackedKeyedFPIndex> newMonkey, Filer newFiler) throws IOException {
        // TODO rework generics to elimnate this cast
        SkipListMapStore.INSTANCE.copyTo(currentFiler, context, newFiler, ((SkipListMapBackedKeyedFPIndex) newMonkey).context, null);
    }

    @Override
    public void release(int alwayRoomForNMoreKeys) {
        MapStore.INSTANCE.release(context.mapContext, alwayRoomForNMoreKeys);
    }

    @Override
    public long get(final byte[] key) throws IOException {
        return backingChunkStore.execute(backingFP, opener, new ChunkTransaction<SkipListMapBackedKeyedFPIndex, Long>() {

            @Override
            public Long commit(SkipListMapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    byte[] got = SkipListMapStore.INSTANCE.getExistingPayload(filer, monkey.context, key);
                    if (got == null) {
                        return -1L;
                    }
                    return FilerIO.bytesLong(got);
                }
            }
        });
    }

    @Override
    public void set(final byte[] key, final long fp) throws IOException {
        backingChunkStore.execute(backingFP, opener, new ChunkTransaction<SkipListMapBackedKeyedFPIndex, Void>() {

            @Override
            public Void commit(SkipListMapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    SkipListMapStore.INSTANCE.add(filer, context, key, FilerIO.longBytes(fp));
                }
                return null;
            }
        });
    }

    @Override
    public long getAndSet(final byte[] key, final long fp) throws IOException {
        return backingChunkStore.execute(backingFP, opener, new ChunkTransaction<SkipListMapBackedKeyedFPIndex, Long>() {

            @Override
            public Long commit(SkipListMapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    byte[] payload = SkipListMapStore.INSTANCE.getExistingPayload(filer, monkey.context, key);
                    long got = -1L;
                    if (payload != null) {
                        got = FilerIO.bytesLong(payload);
                    }
                    SkipListMapStore.INSTANCE.add(filer, context, key, FilerIO.longBytes(fp));
                    return got;
                }
            }
        });
    }

    @Override
    public <H, M, R> R read(ChunkStore chunkStore, byte[] key,
        OpenFiler<M, ChunkFiler> opener, ChunkTransaction<M, R> filerTransaction) throws IOException {
        Object keyLock = keyLocks.lock(key);
        return KeyedFPIndexUtil.INSTANCE.read(this, semaphore, numPermits, chunkStore, keyLock, key, opener, filerTransaction);
    }

    @Override
    public <H, M, R> R writeNewReplace(ChunkStore chunkStore, byte[] key, H hint,
        CreateFiler<H, M, ChunkFiler> creator, OpenFiler<M, ChunkFiler> opener, GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction) throws IOException {
        Object keyLock = keyLocks.lock(key);
        return KeyedFPIndexUtil.INSTANCE.writeNewReplace(this, semaphore, numPermits, chunkStore, keyLock, key, hint, creator, opener,
            growFiler, filerTransaction);
    }

    @Override
    public <H, M, R> R readWriteAutoGrow(ChunkStore chunkStore, byte[] key, H hint,
        CreateFiler<H, M, ChunkFiler> creator, OpenFiler<M, ChunkFiler> opener, GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction) throws IOException {
        Object keyLock = keyLocks.lock(key);
        return KeyedFPIndexUtil.INSTANCE.readWriteAutoGrowIfNeeded(this, semaphore, numPermits, chunkStore, keyLock, key, hint, creator, opener,
            growFiler, filerTransaction);
    }

    @Override
    public boolean stream(final List<KeyRange> ranges, final KeysStream<byte[]> keysStream) throws IOException {
        final MapStore.KeyStream mapKeyStream = new MapStore.KeyStream() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                return keysStream.stream(key);
            }
        };

        return backingChunkStore.execute(backingFP, null, new ChunkTransaction<SkipListMapBackedKeyedFPIndex, Boolean>() {

            @Override
            public Boolean commit(SkipListMapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                return SkipListMapStore.INSTANCE.streamKeys(filer, monkey.context, lock, ranges, mapKeyStream);
            }
        });
    }

}
