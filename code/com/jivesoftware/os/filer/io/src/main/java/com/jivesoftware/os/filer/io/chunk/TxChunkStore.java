/*
 * Copyright 2015 Jive Software.
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
package com.jivesoftware.os.filer.io.chunk;

import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jonathan.colt
 */
public class TxChunkStore implements Copyable<ChunkStore> {

    private final ChunkStore overlay;

    private final ChunkStore master;
    final ConcurrentHashMap<Integer, BlockingQueue<Long>> reusableMasterFP = new ConcurrentHashMap<>();

    private final AtomicBoolean txing = new AtomicBoolean(false);
    private final FPLUT fplut;

    public TxChunkStore(ChunkStore overlay, ChunkStore master, FPLUT fplut) {
        this.overlay = overlay;
        this.master = master;
        this.fplut = fplut;
    }

    static public class FPLUT { // FP look up table

        private final StripingLocksProvider<Long> masterFPLocksProvider;
        final Map<OverlayFP, Long> overlayToMaster = new ConcurrentHashMap<>();
        final Map<Long, OverlayFP> masterToOverlay = new ConcurrentHashMap<>();

        public FPLUT(StripingLocksProvider<Long> masterFPLocksProvider) {
            this.masterFPLocksProvider = masterFPLocksProvider;
        }

        <R> R get(Long masterFP, FPTx<R> fpTx) throws IOException {
            synchronized (masterFPLocksProvider.lock(masterFP)) {
                return fpTx.tx(masterToOverlay.get(masterFP), masterFP);
            }
        }

        void set(OverlayFP overlayFP, Long masterFP) {
            synchronized (masterFPLocksProvider.lock(masterFP)) {
                overlayToMaster.put(overlayFP, masterFP);
                masterToOverlay.put(masterFP, overlayFP);
            }
        }

        <R> R remove(long masterFP, FPTx<R> fpTx) throws IOException {
            synchronized (masterFPLocksProvider.lock(masterFP)) {
                OverlayFP overlayFP = masterToOverlay.get(masterFP);
                R r = fpTx.tx(overlayFP, masterFP);
                overlayToMaster.remove(overlayFP);
                masterToOverlay.remove(masterFP);
                return r;
            }
        }

        boolean contains(long masterFP, FPTx<Boolean> fpTx) throws IOException {
            synchronized (masterFPLocksProvider.lock(masterFP)) {
                return fpTx.tx(masterToOverlay.get(masterFP), masterFP);
            }
        }

        void removeAll(FPTx<Void> fpTx) throws IOException {
            List<OverlayFP> overlayFPs = new ArrayList<>(overlayToMaster.keySet());
            Collections.sort(overlayFPs);
            for (OverlayFP overlayFP : overlayFPs) {
                synchronized (masterFPLocksProvider.lock(overlayFP.masterFP)) {
                    remove(overlayFP.masterFP, fpTx);
                }
            }
        }
    }

    static interface FPTx<R> {

        R tx(OverlayFP overlayFP, Long masterFP) throws IOException;
    }

    @Override
    public void copyTo(ChunkStore to) throws IOException {
        if (txing.compareAndSet(false, true)) {
            commit();
            master.copyTo(to);
            txing.set(false);
        }
    }

    public void begin(Object checkPointId) {
        if (txing.compareAndSet(false, true)) {
        } else {
            throw new IllegalStateException("Called begin on an open transaction.");
        }
    }

    public void commit() throws IOException {

        fplut.removeAll(new FPTx<Void>() {
            @Override
            public Void tx(OverlayFP overlayFP, Long masterFP) throws IOException {
                //master.slabTransfer(overlay, overlayFP.overlayFP, masterFP);
                return null;
            }
        });

        for (Integer chunkPower : reusableMasterFP.keySet()) {
            BlockingQueue<Long> free = reusableMasterFP.get(chunkPower);
            for (Long f = free.poll(); f != null; f = free.poll()) {
                master.remove(f);
            }
        }

        // overlay.reset();
    }

    public <M, H> long newChunk(int level, final H hint, final CreateFiler<H, M, ChunkFiler> createFiler) throws IOException {

        long capacity = createFiler.sizeInBytes(hint);
        int chunkPower = FilerIO.chunkPower(capacity, ChunkStore.cMinPower);
        Long masterFP = null;
        BlockingQueue<Long> free = reusableMasterFP.get(chunkPower);
        if (free != null) {
            masterFP = free.poll();
        }
        if (masterFP == null) {
            masterFP = master.newChunk(hint, createFiler);
        }

        OverlayFP overlayFP = new OverlayFP(level, chunkPower, overlay.newChunk(hint, createFiler), masterFP);
        fplut.set(overlayFP, masterFP);
        return masterFP;
    }

    public <M, R> R readOnly(final int level,
        final long masterFP,
        final OpenFiler<M, ChunkFiler> openFiler,
        final ChunkTransaction<M, R> chunkTransaction) throws IOException {

        return execute(level, masterFP, openFiler, chunkTransaction);
    }

    public <M, R> R readWrite(final int level,
        final long masterFP,
        final OpenFiler<M, ChunkFiler> openFiler,
        final ChunkTransaction<M, R> chunkTransaction) throws IOException {
        return execute(level, masterFP, openFiler, chunkTransaction);
    }

    <M, R> R execute(final int level,
        long masterFP,
        final OpenFiler<M, ChunkFiler> openFiler,
        final ChunkTransaction<M, R> chunkTransaction) throws IOException {

        return fplut.get(masterFP, new FPTx<R>() {

            @Override
            public R tx(OverlayFP overlayFP, final Long masterFP) throws IOException {

                if (overlayFP == null) {
                    overlayFP = master.execute(masterFP, openFiler, new ChunkTransaction<M, OverlayFP>() {

                        @Override
                        public OverlayFP commit(final M monkey, final ChunkFiler masterFiler, Object lock) throws IOException {
                            synchronized (lock) {
                                long size = masterFiler.getSize();
                                int chunkPower = FilerIO.chunkPower(size, ChunkStore.cMinPower); // Grrr
                                long fp = overlay.newChunk(size, new CreateFiler<Long, M, ChunkFiler>() {

                                    @Override
                                    public long sizeInBytes(Long hint) throws IOException {
                                        return hint;
                                    }

                                    @Override
                                    public M create(Long hint, ChunkFiler overlayFiler) throws IOException {
                                        FilerIO.copy(masterFiler, overlayFiler, -1);
                                        return monkey;
                                    }

                                });
                                return new OverlayFP(level, chunkPower, fp, masterFP);
                            }
                        }
                    });
                    fplut.set(overlayFP, overlayFP.masterFP);

                }
                return overlay.execute(overlayFP.overlayFP, openFiler, chunkTransaction);
            }
        });

    }

    public void remove(final long masterFP) throws IOException {
        fplut.remove(masterFP, new FPTx<Void>() {

            @Override
            public Void tx(OverlayFP overlayFP, Long masterFP) throws IOException {
                if (overlayFP != null) {
                    BlockingQueue<Long> free = reusableMasterFP.get(overlayFP.power);
                    if (free == null) {
                        free = new LinkedBlockingQueue<>(Integer.MAX_VALUE);
                        BlockingQueue<Long> had = reusableMasterFP.putIfAbsent(overlayFP.power, free);
                        if (had != null) {
                            free = had;
                        }
                    }
                    free.add(masterFP);

                } else {
                    master.remove(masterFP);
                }
                return null;
            }
        });
    }

    public boolean isValid(long masterFP) throws IOException {
        return fplut.contains(masterFP, new FPTx<Boolean>() {

            @Override
            public Boolean tx(OverlayFP overlayFP, Long masterFP) throws IOException {
                if (overlayFP != null) {
                    return true;
                }
                return master.isValid(masterFP);
            }
        });

    }

    private static class OverlayFP implements Comparable<OverlayFP> {

        public final int level;
        public final int power;
        public final long overlayFP;
        public final long masterFP;

        public OverlayFP(int level, int chunkPower, long chunkFP, long masterFP) {
            this.level = level;
            this.power = chunkPower;
            this.overlayFP = chunkFP;
            this.masterFP = masterFP;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 37 * hash + (int) (this.overlayFP ^ (this.overlayFP >>> 32));
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final OverlayFP other = (OverlayFP) obj;
            if (this.overlayFP != other.overlayFP) {
                return false;
            }
            return true;
        }

        /**
         * Sort based on level descending and then master fp ascending.
         *
         * @param o
         * @return
         */
        @Override
        public int compareTo(OverlayFP o) {
            int c = -Integer.compare(level, o.level);
            if (c == 0) {
                c = Long.compare(masterFP, o.masterFP);
            }
            return c;
        }

    }

}
