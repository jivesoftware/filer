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

import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class ChunkStore implements Copyable<ChunkStore> {

    private static final int maxChunkPower = 32;
    private static ChunkMetrics.ChunkMetric[] allocates = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] gets = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] reuses = new ChunkMetrics.ChunkMetric[maxChunkPower];
    private static ChunkMetrics.ChunkMetric[] removes = new ChunkMetrics.ChunkMetric[maxChunkPower];

    static {
        for (int i = 0; i < maxChunkPower; i++) {
            String size = "2_pow_" + (i > 9 ? i : "0" + i) + "_" + FilerIO.chunkLength(i) + "_bytes";
            allocates[i] = ChunkMetrics.get(size, "allocate");
            gets[i] = ChunkMetrics.get(size, "get");
            reuses[i] = ChunkMetrics.get(size, "reuse");
            removes[i] = ChunkMetrics.get(size, "remove");
        }
    }

    static final long cMagicNumber = Long.MAX_VALUE;
    static final int cMinPower = 8;

    //private final TwoPhasedChunkCache chunkCache;
    private long lengthOfFile = 8 + 8 + (8 * (64 - cMinPower));
    private long referenceNumber = 0;

    //private final Object headerLock = new Object();
    //private AutoGrowingByteBufferBackedFiler filer;
    private StripedFiler filer;

    /*
     New Call Sequence
     ChunkStore chunks = new ChunkStore(locks);
     chunks.setup(100);
     chunks.createAndOpen(_filer);
     */
    //public ChunkStore(ByteBufferFactory byteBufferFactory, int initialCacheSize, int maxNewCacheSize) {
    //this.chunkCache = new TwoPhasedChunkCache(byteBufferFactory, initialCacheSize, maxNewCacheSize);
    //}

    /*
     Existing Call Sequence
     ChunkStore chunks = new ChunkStore(locks, filer);
     chunks.open();
     */
    public ChunkStore(StripedFiler filer) throws Exception {
        this.filer = filer;
    }

    /*
     * file header format
     * lengthOfFile
     * referenceNumber
     * free 2^8
     * free 2^9
     * thru
     * free 2^64
     */
    public void setup(long _referenceNumber) {
        lengthOfFile = 8 + 8 + (8 * (64 - cMinPower));
        referenceNumber = _referenceNumber;
    }

    /**
     * Approximate length (Doesn't lock filer)
     *
     * @return
     * @throws IOException
     */
    public long sizeInBytes() throws IOException {
        return filer.length();
    }

    public void createAndOpen(StripedFiler filer) throws Exception {
        this.filer = filer;
        this.filer.rootTx(-1L, new StripedFiler.StripeTx<Void>() {

            @Override
            public Void tx(long fp, ChunkCache chunkCache, AutoGrowingByteBufferBackedFiler filer) throws IOException {
                filer.seek(0);
                FilerIO.writeLong(filer, lengthOfFile, "lengthOfFile");
                FilerIO.writeLong(filer, referenceNumber, "referenceNumber");
                for (int i = cMinPower; i < 65; i++) {
                    FilerIO.writeLong(filer, -1, "free");
                }
                filer.flush();
                return null;
            }
        });

    }

    public void open() throws IOException {
        this.filer.rootTx(-1L, new StripedFiler.StripeTx<Void>() {

            @Override
            public Void tx(long fp, ChunkCache chunkCache, AutoGrowingByteBufferBackedFiler filer) throws IOException {
                filer.seek(0);
                lengthOfFile = FilerIO.readLong(filer, "lengthOfFile");
                referenceNumber = FilerIO.readLong(filer, "referenceNumber");
                filer.seek(lengthOfFile);
                return null;
            }
        });
    }

    public void delete() throws IOException {

    }

    @Override
    public void copyTo(final ChunkStore to) throws IOException {
        this.filer.rootTx(-1L, new StripedFiler.StripeTx<Void>() {
            @Override
            public Void tx(long fp, ChunkCache chunkCache, final AutoGrowingByteBufferBackedFiler fromFiler) throws IOException {
                to.filer.rootTx(-1L,
                    new StripedFiler.StripeTx<Void>() {

                        @Override
                        public Void tx(long fp, ChunkCache chunkCache, AutoGrowingByteBufferBackedFiler toFiler) throws IOException {
                            fromFiler.seek(0);
                            toFiler.seek(0);
                            //TODO if these filers are both byte buffer backed then it's much faster to do an NIO ByteBuffer.put()
                            FilerIO.copy(fromFiler, toFiler, lengthOfFile, -1);
                            to.open();
                            return null;
                        }
                    });
                return null;
            }
        });
    }

    public void rollCache() throws IOException {
        //chunkCache.roll();
    }

    public long getReferenceNumber() {
        return referenceNumber;
    }

    /**
     * @param <M>
     * @param <H>
     * @param hint
     * @param createFiler Nullable
     * @return
     * @throws IOException
     */
    public <M, H> long newChunk(final H hint, final CreateFiler<H, M, ChunkFiler> createFiler) throws IOException {
        long _capacity = createFiler.sizeInBytes(hint);
        final int chunkPower = FilerIO.chunkPower(_capacity, cMinPower);
        final long chunkLength = FilerIO.chunkLength(chunkPower)
            + 8 // add magicNumber
            + 8 // add chunkPower
            + 8 // add next free chunk of equal size
            + 8; // add bytesLength
        final long chunkPosition = freeSeek(chunkPower);
        final AtomicBoolean reused = new AtomicBoolean(false);
        final AtomicLong chunkFP = new AtomicLong(-1);

        this.filer.rootTx(-1L, new StripedFiler.StripeTx<Void>() {

            @Override
            public Void tx(long fp, ChunkCache chunkCache, AutoGrowingByteBufferBackedFiler filer) throws IOException {
                long reuseFp = reuseChunk(filer, chunkPosition);

                if (reuseFp == -1) {
                    long newChunkFP = lengthOfFile;
                    filer.seek(newChunkFP + chunkLength - 1); // last byte in chunk
                    filer.write(0); // cause file backed ChunkStore to grow file on disk. Use setLength()?
                    filer.seek(newChunkFP);
                    FilerIO.writeLong(filer, cMagicNumber, "magicNumber");
                    FilerIO.writeLong(filer, chunkPower, "chunkPower");
                    FilerIO.writeLong(filer, -1, "chunkNexFreeChunkFP");
                    FilerIO.writeLong(filer, chunkLength, "chunkLength");
                    lengthOfFile += chunkLength;
                    filer.seek(lengthOfFile); //  force allocation of space
                    filer.seek(0);
                    FilerIO.writeLong(filer, lengthOfFile, "lengthOfFile");
                    filer.flush();
                    reuseFp = newChunkFP;
                    reused.set(true);
                }
                chunkFP.set(reuseFp);
                return null;
            }
        });

        if (reused.get()) {
            reuses[chunkPower].inc(1);
        } else {
            allocates[chunkPower].inc(1);
        }

        filer.tx(chunkFP.get(), new StripedFiler.StripeTx<Void>() {

            @Override
            public Void tx(long fp, ChunkCache chunkCache, AutoGrowingByteBufferBackedFiler filer) throws IOException {
                filer.seek(fp);
                long magicNumber = FilerIO.readLong(filer, "magicNumber");
                if (magicNumber != cMagicNumber) {
                    throw new IOException("Invalid chunkFP " + fp);
                }
                int chunkPower = (int) FilerIO.readLong(filer, "chunkPower");
                FilerIO.readLong(filer, "chunkNexFreeChunkFP");
                FilerIO.readLong(filer, "chunkLength");
                long startOfFP = filer.getFilePointer();
                long endOfFP = startOfFP + FilerIO.chunkLength(chunkPower);
                ChunkFiler chunkFiler = new ChunkFiler(ChunkStore.this, filer.duplicate(startOfFP, endOfFP), fp, startOfFP, endOfFP);
                chunkFiler.seek(0);
                M monkey = createFiler.create(hint, chunkFiler);
                chunkCache.set(fp, new Chunk<>(monkey, fp, startOfFP, endOfFP), 2);
                return null;
            }
        });
        return chunkFP.get();
    }

    /**
     * Synchronize externally on filer.lock()
     */
    private long reuseChunk(Filer filer, long position) throws IOException {
        filer.seek(position);
        long reuseFP = FilerIO.readLong(filer, "free");
        if (reuseFP == -1) {
            return reuseFP;
        }
        long nextFree = readNextFree(filer, reuseFP);
        filer.seek(position);
        FilerIO.writeLong(filer, nextFree, "free");
        return reuseFP;
    }

    /**
     * Synchronize externally on filer.lock()
     */
    private long readNextFree(Filer filer, long _chunkFP) throws IOException {
        filer.seek(_chunkFP);
        FilerIO.readLong(filer, "magicNumber");
        FilerIO.readLong(filer, "chunkPower");
        return FilerIO.readLong(filer, "chunkNexFreeChunkFP");
    }

    /**
     * Synchronize externally on filer.lock()
     */
    private void writeNextFree(Filer filer, long _chunkFP, long _nextFreeFP) throws IOException {
        filer.seek(_chunkFP);
        FilerIO.readLong(filer, "magicNumber");
        FilerIO.readLong(filer, "chunkPower");
        FilerIO.writeLong(filer, _nextFreeFP, "chunkNexFreeChunkFP");
    }

    /**
     * @param <M>
     * @param <R>
     * @param chunkFP
     * @param openFiler
     * @param chunkTransaction
     * @return
     * @throws IOException
     */
    public <M, R> R execute(final long chunkFP, final OpenFiler<M, ChunkFiler> openFiler, final ChunkTransaction<M, R> chunkTransaction) throws IOException {

        final Chunky<M> chunky = filer.tx(chunkFP, new StripedFiler.StripeTx<Chunky<M>>() {

            @Override
            public Chunky<M> tx(long fp, ChunkCache chunkCache, final AutoGrowingByteBufferBackedFiler filer) throws IOException {
                Chunk<M> chunk = chunkCache.acquireIfPresent(chunkFP);
                if (chunk == null) {
                    filer.seek(chunkFP);
                    long magicNumber = FilerIO.readLong(filer, "magicNumber");
                    if (magicNumber != cMagicNumber) {
                        throw new IOException("Invalid chunkFP " + chunkFP);
                    }
                    int chunkPower = (int) FilerIO.readLong(filer, "chunkPower");
                    FilerIO.readLong(filer, "chunkNexFreeChunkFP");
                    FilerIO.readLong(filer, "chunkLength");
                    long startOfFP = filer.getFilePointer();

                    long endOfFP = startOfFP + FilerIO.chunkLength(chunkPower);
                    ChunkFiler chunkFiler = new ChunkFiler(ChunkStore.this, filer.duplicate(startOfFP, endOfFP), chunkFP, startOfFP, endOfFP);
                    chunkFiler.seek(0);

                    M monkey = openFiler.open(chunkFiler);
                    chunk = new Chunk<>(monkey, chunkFP, startOfFP, endOfFP);
                    chunkCache.promoteAndAcquire(chunkFP, chunk, 2);
                }

                ChunkFiler chunkFiler = new ChunkFiler(ChunkStore.this, filer.duplicate(chunk.startOfFP, chunk.endOfFP), chunkFP, chunk.startOfFP,
                    chunk.endOfFP);
                chunkFiler.seek(0);
                return new Chunky<>(chunkFiler, chunk);
            }
        });

        try {
            return chunkTransaction.commit(chunky.chunk.monkey, chunky.filer, chunky.chunk);
        } finally {

            filer.tx(chunkFP, new StripedFiler.StripeTx<Void>() {

                @Override
                public Void tx(long fp, ChunkCache chunkCache, AutoGrowingByteBufferBackedFiler filer) throws IOException {
                    chunkCache.release(chunkFP);
                    return null;
                }
            });

        }
    }

    private static class Chunky<M> {

        final ChunkFiler filer;
        final Chunk<M> chunk;

        public Chunky(ChunkFiler chunky, Chunk<M> monkey) {
            this.filer = chunky;
            this.chunk = monkey;
        }

    }

    public void remove(long chunkFP) throws IOException {

        final Integer chunkPower = filer.tx(chunkFP, new StripedFiler.StripeTx<Integer>() {

            @Override
            public Integer tx(long fp, ChunkCache chunkCache, AutoGrowingByteBufferBackedFiler filer) throws IOException {
                chunkCache.remove(fp);

                filer.seek(fp);
                long magicNumber = FilerIO.readLong(filer, "magicNumber");
                if (magicNumber != cMagicNumber) {
                    throw new IOException("Invalid chunkFP " + fp);
                }
                int chunkPower = (int) FilerIO.readLong(filer, "chunkPower");
                FilerIO.readLong(filer, "chunkNexFreeChunkFP");
                FilerIO.writeLong(filer, -1, "chunkLength");
                long chunkLength = FilerIO.chunkLength(chunkPower); // bytes
                // fill with zeros
                while (chunkLength >= zerosMax.length) {
                    filer.write(zerosMax);
                    chunkLength -= zerosMax.length;
                }
                while (chunkLength >= zerosMin.length) {
                    filer.write(zerosMin);
                    chunkLength -= zerosMin.length;
                }
                filer.flush();
                return chunkPower;
            }
        });

        filer.rootTx(chunkFP, new StripedFiler.StripeTx<Void>() {

            @Override
            public Void tx(long fp, ChunkCache chunkCache, AutoGrowingByteBufferBackedFiler filer) throws IOException {

                // save as free chunk
                long position = freeSeek(chunkPower);
                filer.seek(position);
                long freeFP = FilerIO.readLong(filer, "free");
                if (freeFP == -1) {
                    filer.seek(position);
                    FilerIO.writeLong(filer, fp, "free");
                } else {
                    if (fp != freeFP) {
                        filer.seek(position);
                        FilerIO.writeLong(filer, fp, "free");
                    } else {
                        System.err.println("WARNING: Some one is removing the same chunk more than once. chunkFP:" + fp);
                        new RuntimeException().printStackTrace();
                    }
                }
                writeNextFree(filer, fp, freeFP);
                filer.flush();
                return null;
            }
        });

        removes[chunkPower].inc(1);
    }

    private long freeSeek(long _chunkPower) {
        return 8 + 8 + ((_chunkPower - cMinPower) * 8);
    }

    private static final byte[] zerosMin = new byte[(int) Math.pow(2, cMinPower)]; // never too big
    private static final byte[] zerosMax = new byte[(int) Math.pow(2, 16)]; // 65536 max used until min needed

    public boolean isValid(final long chunkFP) throws IOException {
        return filer.tx(chunkFP, new StripedFiler.StripeTx<Boolean>() {

            @Override
            public Boolean tx(long fp, ChunkCache chunkCache, AutoGrowingByteBufferBackedFiler filer) throws IOException {
                if (chunkCache.contains(fp)) {
                    return true;
                }
                filer.seek(fp);
                long magicNumber = FilerIO.readLong(filer, "magicNumber");
                return magicNumber == cMagicNumber;
            }
        });

    }

}
