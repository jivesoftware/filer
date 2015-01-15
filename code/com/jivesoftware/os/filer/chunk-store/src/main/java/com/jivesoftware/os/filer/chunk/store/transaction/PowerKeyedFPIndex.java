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
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;
import java.io.IOException;
import java.util.concurrent.Semaphore;

/**
 * @author jonathan.colt
 */
public class PowerKeyedFPIndex implements KeyedFPIndexUtil.BackingFPIndex<Integer> {

    private final ChunkStore backingChunkStore;
    private final long backingFP;
    private final int numPermits = 64; //TODO expose?

    private final long[] fpIndex;
    private final Semaphore semaphore = new Semaphore(numPermits, true);
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
    synchronized public long get(Integer key) throws IOException {
        return fpIndex[key];
    }

    @Override
    synchronized public void set(final Integer key, final long fp) throws IOException {
        backingChunkStore.execute(backingFP, null, new ChunkTransaction<PowerKeyedFPIndex, Void>() {

            @Override
            public Void commit(PowerKeyedFPIndex monkey, ChunkFiler filer) throws IOException {
                synchronized (monkey) {
                    filer.seek(8 + (8 * key));
                    FilerIO.writeLong(filer, fp, "fp");
                }
                return null;
            }
        });
        fpIndex[key] = fp;
    }

    public <H, M, R> R commit(ChunkStore chunkStore,
        Integer keySize,
        H hint,
        CreateFiler<H, M, ChunkFiler> creator,
        OpenFiler<M, ChunkFiler> opener,
        GrowFiler<H, M, ChunkFiler> growFiler,
        ChunkTransaction<M, R> filerTransaction) throws IOException {

        return KeyedFPIndexUtil.INSTANCE.commit(this, semaphore, numPermits, chunkStore, keySizeLocks[keySize], keySize,
            hint, creator, opener, growFiler, filerTransaction);

    }

    synchronized public Boolean stream(final KeysStream<Integer> keysStream) throws IOException {
        for (int i = 0; i < fpIndex.length; i++) {
            if (fpIndex[i] > -1 && !keysStream.stream(i)) {
                return false;
            }
        }
        return true;
    }

}
