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
import com.jivesoftware.os.filer.chunk.store.transaction.PowerToFPLevel.PowerFPs;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.OpenFiler;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class PowerToFPLevel implements LevelProvider<Void, Long, PowerFPs> {

    private static final long MAGIC_SKY_HOOK_NUMBER = 5583112375L;
    private final int skyHookMaxKeySizePower = 16;
    private final ChunkStore chunkStore;

    public PowerToFPLevel(ChunkStore chunkStore) {
        this.chunkStore = chunkStore;
    }

    long initialSize() {
        return 8 + (8 * skyHookMaxKeySizePower);
    }

    @Override
    public <R> R acquire(Void parent, Long fp, final StoreTransaction<R, PowerFPs> storeTransaction) throws IOException {
        return chunkStore.getOrCreate(fp, initialSize(), creator(), opener(), new ChunkTransaction<PowerFPs, R>() {
            @Override
            public R commit(PowerFPs monkey, ChunkFiler filer) throws IOException {
                return storeTransaction.commit(monkey);
            }
        });
    }

    @Override
    public void release(Void parentStore, Long fp, final PowerFPs skyHookFPs) throws IOException {
        if (skyHookFPs.updated) {
            chunkStore.execute(fp, opener(), new ChunkTransaction<PowerFPs, Void>() {
                @Override
                public Void commit(PowerFPs monkey, ChunkFiler filer) throws IOException {
                    filer.seek(8);
                    filer.write(FilerIO.longsBytes(skyHookFPs.powerFPs));
                    return null;
                }
            });
        }
    }

    CreateFiler<PowerFPs, ChunkFiler> creator() {
        return new CreateFiler<PowerFPs, ChunkFiler>() {
            @Override
            public PowerFPs create(ChunkFiler filer) throws IOException {
                filer.seek(0);
                FilerIO.writeLong(filer, MAGIC_SKY_HOOK_NUMBER, "magicHeader");
                for (int i = 0; i < skyHookMaxKeySizePower; i++) {
                    FilerIO.writeLong(filer, -1, "powerFP");
                }
                long[] fps = new long[skyHookMaxKeySizePower];
                Arrays.fill(fps, -1L);
                return new PowerFPs(fps); // monkey state cache with chunkFiler
            }
        };
    }

    OpenFiler<PowerFPs, ChunkFiler> opener() {
        return new OpenFiler<PowerFPs, ChunkFiler>() {
            @Override
            public PowerFPs open(ChunkFiler filer) throws IOException {
                filer.seek(0);
                long magicNumber = FilerIO.readLong(filer, "magicHeader");
                if (magicNumber != MAGIC_SKY_HOOK_NUMBER) {
                    throw new IllegalStateException("Expected magicHeader:" + MAGIC_SKY_HOOK_NUMBER + " but found:" + magicNumber);
                }
                byte[] rawFPS = new byte[8 * skyHookMaxKeySizePower];
                FilerIO.read(filer, rawFPS);
                long[] fps = FilerIO.bytesLongs(rawFPS);
                return new PowerFPs(fps); // monkey state cache with chunkFiler
            }
        };
    }

    static public class PowerFPs implements KeyedFP<Integer> {

        // Monkey
        final long[] powerFPs;
        boolean updated;

        PowerFPs(long[] powerFPs) {
            this.powerFPs = powerFPs;
        }

        @Override
        public void update(Integer keySize, long fp) {
            if (powerFPs[keySize] != fp) {
                powerFPs[keySize] = fp;
                updated = true;
            }
        }

        @Override
        public long getFP(Integer keySize) {
            return powerFPs[keySize];
        }

        @Override
        public String toString() {
            return "SkyHookFPs{" + "powerFPs=" + Arrays.toString(powerFPs) + ", updated=" + updated + '}';
        }

    }

}
