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
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;
import java.util.concurrent.Semaphore;

/**
 * @author jonathan.colt
 */
public class MapBackedKeyedFPIndex implements KeyedFPIndexUtil.BackingFPIndex<byte[]> {

    private final ChunkStore backingChunkStore;
    private final long backingFP;
    private final int numPermits = 64; // TODO expose ?
    private final Semaphore semaphore = new Semaphore(numPermits, true);
    private final MapContext mapContext;
    private final MapBackedKeyedFPIndexOpener opener;
    private final ByteArrayStripingLocksProvider keyLocks;

    public MapBackedKeyedFPIndex(ChunkStore chunkStore,
        long fp,
        MapContext mapContext,
        MapBackedKeyedFPIndexOpener opener,
        ByteArrayStripingLocksProvider keyLocks) {

        this.backingChunkStore = chunkStore;
        this.backingFP = fp;
        this.mapContext = mapContext;
        this.opener = opener;
        this.keyLocks = keyLocks;
    }

    MapContext getMapContext() {
        return mapContext;
    }

    @Override
    public long get(final byte[] key) throws IOException {
        return backingChunkStore.execute(backingFP, opener, new ChunkTransaction<MapBackedKeyedFPIndex, Long>() {

            @Override
            public Long commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    long ai = MapStore.INSTANCE.get(filer, monkey.mapContext, key);
                    if (ai < 0) {
                        return -1L;
                    }
                    return FilerIO.bytesLong(MapStore.INSTANCE.getPayload(filer, monkey.mapContext, ai));
                }
            }
        });
    }

    @Override
    public void set(final byte[] key, final long fp) throws IOException {
        backingChunkStore.execute(backingFP, opener, new ChunkTransaction<MapBackedKeyedFPIndex, Void>() {

            @Override
            public Void commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    MapStore.INSTANCE.add(filer, monkey.mapContext, (byte) 1, key, FilerIO.longBytes(fp));
                }
                return null;
            }
        });
    }

    public <H, M, R> R commit(ChunkStore chunkStore,
        byte[] key,
        H hint,
        CreateFiler<H, M, ChunkFiler> creator,
        OpenFiler<M, ChunkFiler> opener,
        GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction) throws IOException {
        Object keyLock = keyLocks.lock(key);
        return KeyedFPIndexUtil.INSTANCE.commit(this, semaphore, numPermits, chunkStore, keyLock, key, hint, creator, opener, growFiler, filerTransaction);
    }

    public Boolean stream(final KeysStream<byte[]> keysStream) throws IOException {
        final MapStore.KeyStream mapKeyStream = new MapStore.KeyStream() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                return keysStream.stream(key);
            }
        };

        return backingChunkStore.execute(backingFP, null, new ChunkTransaction<MapBackedKeyedFPIndex, Boolean>() {

            @Override
            public Boolean commit(MapBackedKeyedFPIndex monkey, ChunkFiler filer, Object lock) throws IOException {
                return MapStore.INSTANCE.streamKeys(filer, monkey.mapContext, lock, mapKeyStream);
            }
        });
    }

}
