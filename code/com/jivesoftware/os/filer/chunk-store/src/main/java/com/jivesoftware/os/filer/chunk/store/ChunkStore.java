package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.chunk.store.ChunkMetrics.ChunkMetric;
import com.jivesoftware.os.filer.io.ConcurrentFiler;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.SubsetableFiler;
import java.io.IOException;

public class ChunkStore implements Copyable<ChunkStore, Exception> {

    private static final int maxChunkPower = 32;
    private static ChunkMetric[] allocates = new ChunkMetric[maxChunkPower];
    private static ChunkMetric[] gets = new ChunkMetric[maxChunkPower];
    private static ChunkMetric[] reuses = new ChunkMetric[maxChunkPower];
    private static ChunkMetric[] removes = new ChunkMetric[maxChunkPower];

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

    private long lengthOfFile = 8 + 8 + (8 * (64 - cMinPower));
    private long referenceNumber = 0;

    private ConcurrentFiler filer;

    private final StripingLocksProvider<Long> locksProvider = new StripingLocksProvider<>(1024);
    /*
     New Call Sequence
     ChunkStore chunks = ChunkStore();
     chunks.setup(100);
     open(_filer);
     */
    public ChunkStore() {
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

    public long sizeInBytes() throws IOException {
        return filer.length();
    }

    public long bytesNeeded() {
        return Long.MAX_VALUE;
    }

    public void createAndOpen(ConcurrentFiler _filer) throws Exception {
        filer = _filer;
        synchronized (filer.lock()) {
            FilerIO.writeLong(filer, lengthOfFile, "lengthOfFile");
            FilerIO.writeLong(filer, referenceNumber, "referenceNumber");
            for (int i = cMinPower; i < 65; i++) {
                FilerIO.writeLong(filer, -1, "free");
            }
            filer.flush();
        }
    }

    /*
     Exsisting Call Sequence
     ChunkStore chunks = ChunkStore(_filer);
     open();
     */
    public ChunkStore(ConcurrentFiler filer) throws Exception {
        this.filer = filer;
    }

    public void open() throws Exception {
        synchronized (filer.lock()) {
            filer.seek(0);
            lengthOfFile = FilerIO.readLong(filer, "lengthOfFile");
            referenceNumber = FilerIO.readLong(filer, "referenceNumber");
        }
    }

    @Override
    public void copyTo(ChunkStore to) throws Exception {
        synchronized (filer.lock()) {
            filer.seek(0);
            to.copyFrom(filer);
        }
    }

    private void copyFrom(Filer from) throws Exception {
        synchronized (filer.lock()) {
            filer.seek(0);
            FilerIO.copy(from, filer, -1); //TODO expose?
            open();
        }
    }

    public long getReferenceNumber() {
        return referenceNumber;
    }

    public void allChunks(ChunkIdStream _chunks) throws Exception {
        synchronized (filer.lock()) {
            filer.seek(8 + 8 + (8 * (64 - cMinPower)));
            long size = filer.length();
            while (filer.getFilePointer() < size) {
                long chunkFP = filer.getFilePointer();
                long magicNumber = FilerIO.readLong(filer, "magicNumber");
                if (magicNumber != cMagicNumber) {
                    throw new Exception("Invalid chunkFP " + chunkFP);
                }
                long chunkPower = FilerIO.readLong(filer, "chunkPower");
                FilerIO.readLong(filer, "chunkNexFreeChunkFP");
                long chunkLength = FilerIO.readLong(filer, "chunkLength");
                long fp = filer.getFilePointer();
                if (chunkLength > 0) {
                    long more = _chunks.stream(chunkFP);
                    if (more != chunkFP) {
                        break;
                    }
                }
                filer.seek(fp + FilerIO.chunkLength((int) chunkPower));
            }
        }
    }

    public long newChunk(long _capacity) throws Exception {
        int chunkPower = FilerIO.chunkPower(_capacity, cMinPower);
        long chunkLength = FilerIO.chunkLength(chunkPower);
        chunkLength += 8; // add magicNumber
        chunkLength += 8; // add chunkPower
        chunkLength += 8; // add next free chunk of equal size
        chunkLength += 8; // add bytesLength
        long chunkPosition = freeSeek(chunkPower);
        boolean reused = false;
        long chunkFP;
        synchronized (filer.lock()) {
            long resultFP = reuseChunk(chunkPosition);
            if (resultFP == -1) {
                long newChunkFP = lengthOfFile;
                if (newChunkFP + chunkLength > filer.capacity()) {
                    //!! to do over flow allocated chunk request reallocation
                    throw new RuntimeException("need larger allocation for ChunkFile" + this);
                }
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
            } else {
                chunkFP = resultFP;
            }
        }
        if (reused) {
            reuses[chunkPower].inc(1);
        } else {
            allocates[chunkPower].inc(1);
        }
        return chunkFP;

    }

    /**
     * Synchronize externally on filer.lock()
     */
    private long reuseChunk(long position) throws Exception {
        filer.seek(position);
        long reuseFP = FilerIO.readLong(filer, "free");
        if (reuseFP == -1) {
            return reuseFP;
        }
        long nextFree = readNextFree(reuseFP);
        filer.seek(position);
        FilerIO.writeLong(filer, nextFree, "free");
        return reuseFP;
    }

    public Filer getFiler(long _chunkFP) throws Exception {
        int chunkPower = 0;
        long nextFreeChunkFP = 0;
        long length = 0;
        long fp = 0;
        synchronized (filer.lock()) {
            filer.seek(_chunkFP);
            long magicNumber = FilerIO.readLong(filer, "magicNumber");
            if (magicNumber != cMagicNumber) {
                throw new Exception("Invalid chunkFP " + _chunkFP);
            }
            chunkPower = (int) FilerIO.readLong(filer, "chunkPower");
            nextFreeChunkFP = FilerIO.readLong(filer, "chunkNexFreeChunkFP");
            length = FilerIO.readLong(filer, "chunkLength");
            fp = filer.getFilePointer();
        }

        gets[chunkPower].inc(1);

        try {
            Object lock = locksProvider.lock(fp);
            Filer asConcurrentReadWrite = filer.asConcurrentReadWrite(lock);
            return new SubsetableFiler(asConcurrentReadWrite, fp, fp + FilerIO.chunkLength((int) chunkPower), length);
        } catch (Exception x) {
            x.printStackTrace();
            System.out.println("_chunkFP=" + _chunkFP);
            System.out.println("nextFree=" + nextFreeChunkFP);
            System.out.println("fp=" + fp);
            System.out.println("length=" + length);
            System.out.println("chunkPower=" + chunkPower);
            throw x;
        }
    }

    public void remove(long _chunkFP) throws Exception {
        int chunkPower;
        synchronized (filer.lock()) {
            filer.seek(_chunkFP);
            long magicNumber = FilerIO.readLong(filer, "magicNumber");
            if (magicNumber != cMagicNumber) {
                throw new Exception("Invalid chunkFP " + _chunkFP);
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
                filer.flush();
            } else {
                long nextFree = readNextFree(freeFP);
                filer.seek(position);
                FilerIO.writeLong(filer, _chunkFP, "free");
                writeNextFree(_chunkFP, nextFree);
                filer.flush();
            }
        }
        removes[chunkPower].inc(1);
    }

    private long freeSeek(long _chunkPower) {
        return 8 + 8 + ((_chunkPower - cMinPower) * 8);
    }

    private static final byte[] zerosMin = new byte[(int) Math.pow(2, cMinPower)]; // never too big
    private static final byte[] zerosMax = new byte[(int) Math.pow(2, 16)]; // 65536 max used until min needed

    /**
     * Synchronize externally on filer.lock()
     */
    private long readNextFree(long _chunkFP) throws Exception {
        filer.seek(_chunkFP);
        FilerIO.readLong(filer, "magicNumber");
        FilerIO.readLong(filer, "chunkPower");
        return FilerIO.readLong(filer, "chunkNexFreeChunkFP");
    }

    /**
     * Synchronize externally on filer.lock()
     */
    private void writeNextFree(long _chunkFP, long _nextFreeFP) throws Exception {
        filer.seek(_chunkFP);
        FilerIO.readLong(filer, "magicNumber");
        FilerIO.readLong(filer, "chunkPower");
        FilerIO.writeLong(filer, _nextFreeFP, "chunkNexFreeChunkFP");
    }

}
