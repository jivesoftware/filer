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
package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import java.io.IOException;

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

    private static final long cMagicNumber = Long.MAX_VALUE;
    private static final int cMinPower = 8;

    //private final ConcurrentHashMap<Long, Chunk<?>> openChunks = new ConcurrentHashMap<>();
    private final ChunkCache openChunks = new ChunkCache(new byte[]{0}, new HeapByteBufferFactory()); // Consider Direct
    private final StripingLocksProvider<Long> locksProvider;

    private long lengthOfFile = 8 + 8 + (8 * (64 - cMinPower));
    private long referenceNumber = 0;

    private final Object headerLock = new Object();
    private AutoGrowingByteBufferBackedFiler filer;

    /*
     New Call Sequence
     ChunkStore chunks = new ChunkStore(locks);
     chunks.setup(100);
     chunks.createAndOpen(_filer);
     */
    public ChunkStore(StripingLocksProvider<Long> locksProvider) {
        this.locksProvider = locksProvider;
    }

    /*
     Existing Call Sequence
     ChunkStore chunks = new ChunkStore(locks, filer);
     chunks.open();
     */
    public ChunkStore(StripingLocksProvider<Long> locksProvider, AutoGrowingByteBufferBackedFiler filer) throws Exception {
        this(locksProvider);
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

    public void createAndOpen(AutoGrowingByteBufferBackedFiler filer) throws Exception {
        this.filer = filer;
        synchronized (headerLock) {
            filer.seek(0);
            FilerIO.writeLong(filer, lengthOfFile, "lengthOfFile");
            FilerIO.writeLong(filer, referenceNumber, "referenceNumber");
            for (int i = cMinPower; i < 65; i++) {
                FilerIO.writeLong(filer, -1, "free");
            }
            filer.flush();
        }
    }

    public void open() throws IOException {
        synchronized (headerLock) {
            filer.seek(0);
            lengthOfFile = FilerIO.readLong(filer, "lengthOfFile");
            referenceNumber = FilerIO.readLong(filer, "referenceNumber");
        }
    }

    public void delete() throws IOException {

    }

    @Override
    public void copyTo(final ChunkStore to) throws IOException {
        synchronized (headerLock) {
            synchronized (to.headerLock) {
                filer.seek(0);
                to.filer.seek(0);
                //TODO if these filers are both byte buffer backed then it's much faster to do an NIO ByteBuffer.put()
                FilerIO.copy(filer, to.filer, -1);
                to.open();
            }
        }
    }

    public long getReferenceNumber() {
        return referenceNumber;
    }

    public void allChunks(final ChunkIdStream chunkIdStream) throws IOException {
        filer.seek(8 + 8 + (8 * (64 - cMinPower)));
        long size = filer.length();
        while (filer.getFilePointer() < size) {
            long chunkFP = filer.getFilePointer();
            long magicNumber = FilerIO.readLong(filer, "magicNumber");
            if (magicNumber != cMagicNumber) {
                throw new IOException("Invalid chunkFP " + chunkFP);
            }
            long chunkPower = FilerIO.readLong(filer, "chunkPower");
            FilerIO.readLong(filer, "chunkNexFreeChunkFP");
            long chunkLength = FilerIO.readLong(filer, "chunkLength");
            long fp = filer.getFilePointer();
            if (chunkLength > 0) {
                long more = chunkIdStream.stream(chunkFP);
                if (more != chunkFP) {
                    break;
                }
            }
            filer.seek(fp + FilerIO.chunkLength((int) chunkPower));
        }
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
        int chunkPower = FilerIO.chunkPower(_capacity, cMinPower);
        final long chunkLength = FilerIO.chunkLength(chunkPower)
            + 8 // add magicNumber
            + 8 // add chunkPower
            + 8 // add next free chunk of equal size
            + 8; // add bytesLength
        final long chunkPosition = freeSeek(chunkPower);
        boolean reused = false;
        long chunkFP;

        // first try to reuse
        synchronized (headerLock) {
            chunkFP = reuseChunk(filer, chunkPosition);

            if (chunkFP == -1) {
                final long newChunkFP = lengthOfFile;
                long newSize = newChunkFP + chunkLength;
                filer.seek(newChunkFP + chunkLength - 1); // last byte in chunk
                filer.write(0); // cause file backed ChunkStore to grow file on disk. Use setLength()?
                filer.seek(newChunkFP);
                FilerIO.writeLong(filer, cMagicNumber, "magicNumber");
                FilerIO.writeLong(filer, chunkPower, "chunkPower");
                FilerIO.writeLong(filer, -1, "chunkNexFreeChunkFP");
                FilerIO.writeLong(filer, chunkLength, "chunkLength");
                lengthOfFile += chunkLength;
                filer.seek(0);
                FilerIO.writeLong(filer, lengthOfFile, "lengthOfFile");
                filer.flush();
                chunkFP = newChunkFP;
                reused = true;
            }
        }

        if (reused) {
            reuses[chunkPower].inc(1);
        } else {
            allocates[chunkPower].inc(1);
        }

        long _chunkFP = chunkFP;
        long startOfFP;
        long endOfFP;
        synchronized (headerLock) {
            filer.seek(_chunkFP);
            long magicNumber = FilerIO.readLong(filer, "magicNumber");
            if (magicNumber != cMagicNumber) {
                throw new IOException("Invalid chunkFP " + _chunkFP);
            }
            chunkPower = (int) FilerIO.readLong(filer, "chunkPower");
            FilerIO.readLong(filer, "chunkNexFreeChunkFP");
            FilerIO.readLong(filer, "chunkLength");
            startOfFP = filer.getFilePointer();
            endOfFP = startOfFP + FilerIO.chunkLength(chunkPower);
        }
        ChunkFiler chunkFiler = new ChunkFiler(this, filer.duplicate(startOfFP, endOfFP), _chunkFP, startOfFP, endOfFP);
        chunkFiler.seek(0);
        M monkey = createFiler.create(hint, chunkFiler);
        if (monkey == null) {
            throw new IllegalStateException("Monkey cannot be null");
        }
        openChunks.set(_chunkFP, startOfFP, endOfFP, monkey);
        return chunkFP;
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

        Chunk<M> chunk = (Chunk<M>) openChunks.get(chunkFP);
        ChunkFiler chunkFiler = null;
        if (chunk == null) {
            int chunkPower;
            long startOfFP;
            synchronized (headerLock) {
                filer.seek(chunkFP);
                long magicNumber = FilerIO.readLong(filer, "magicNumber");
                if (magicNumber != cMagicNumber) {
                    throw new IOException("Invalid chunkFP " + chunkFP);
                }
                chunkPower = (int) FilerIO.readLong(filer, "chunkPower");
                FilerIO.readLong(filer, "chunkNexFreeChunkFP");
                FilerIO.readLong(filer, "chunkLength");
                startOfFP = filer.getFilePointer();
            }

            long endOfFP = startOfFP + FilerIO.chunkLength(chunkPower);
            chunkFiler = new ChunkFiler(this, filer.duplicate(startOfFP, endOfFP), chunkFP, startOfFP, endOfFP);
            chunkFiler.seek(0);

            //TODO replace in java8 with computeIfAbsent (guava cache is too fat)
            synchronized (locksProvider.lock(chunkFP)) {
                if (!openChunks.contains(chunkFP)) {
                    M monkey = openFiler.open(chunkFiler);
                    openChunks.set(chunkFP, startOfFP, endOfFP, monkey);
                }
            }
        }
        if (chunk == null) {
            chunk = (Chunk<M>) openChunks.get(chunkFP);
        }
        if (chunkFiler == null) {
            chunkFiler = new ChunkFiler(ChunkStore.this, filer.duplicate(chunk.startOfFP, chunk.endOfFP), chunkFP, chunk.startOfFP, chunk.endOfFP);
        }

        chunkFiler.seek(0);
        return chunkTransaction.commit(chunk.monkey, chunkFiler);
    }

    public void remove(final long _chunkFP) throws IOException {
        int chunkPower;
        openChunks.remove(_chunkFP);
        synchronized (headerLock) {
            filer.seek(_chunkFP);
            long magicNumber = FilerIO.readLong(filer, "magicNumber");
            if (magicNumber != cMagicNumber) {
                throw new IOException("Invalid chunkFP " + _chunkFP);
            }
            chunkPower = (int) FilerIO.readLong(filer, "chunkPower");
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
            // save as free chunk
            long position = freeSeek(chunkPower);
            filer.seek(position);
            long freeFP = FilerIO.readLong(filer, "free");
            if (freeFP == -1) {
                filer.seek(position);
                FilerIO.writeLong(filer, _chunkFP, "free");
            } else {
                if (_chunkFP != freeFP) {
                    filer.seek(position);
                    FilerIO.writeLong(filer, _chunkFP, "free");
                } else {
                    System.err.println("WARNING: Some one is removing the same chunk more than once. chunkFP:" + _chunkFP);
                    new RuntimeException().printStackTrace();
                }
            }
            writeNextFree(filer, _chunkFP, freeFP);
            filer.flush();

        }
        removes[chunkPower].inc(1);
    }

    private long freeSeek(long _chunkPower) {
        return 8 + 8 + ((_chunkPower - cMinPower) * 8);
    }

    private static final byte[] zerosMin = new byte[(int) Math.pow(2, cMinPower)]; // never too big
    private static final byte[] zerosMax = new byte[(int) Math.pow(2, 16)]; // 65536 max used until min needed

    public boolean isValid(final long chunkFP) throws IOException {
        if (openChunks.get(chunkFP) != null) {
            return true;
        }
        synchronized (headerLock) {
            filer.seek(chunkFP);
            long magicNumber = FilerIO.readLong(filer, "magicNumber");
            return magicNumber == cMagicNumber;
        }

    }

}
