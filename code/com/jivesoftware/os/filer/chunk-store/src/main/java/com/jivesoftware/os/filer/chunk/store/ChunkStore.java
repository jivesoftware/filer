package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.chunk.store.ChunkMetrics.ChunkMetric;
import com.jivesoftware.os.filer.io.Copyable;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.OpenFiler;
import com.jivesoftware.os.filer.io.ResizableByteBuffer;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkStore implements Copyable<ChunkStore> {

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

    private final ConcurrentHashMap<Long, Chunk<?>> openChunks = new ConcurrentHashMap<>();

    private long lengthOfFile = 8 + 8 + (8 * (64 - cMinPower));
    private long referenceNumber = 0;

    private final Object headerLock = new Object();
    private ResizableByteBuffer resizableByteBuffer;

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

    private final FilerTransaction<Filer, Long> sizeInBytesTransaction = new FilerTransaction<Filer, Long>() {
        @Override
        public Long commit(Filer filer) throws IOException {
            return filer.length();
        }
    };

    public long sizeInBytes() throws IOException {
        return resizableByteBuffer.execute(-1, false, sizeInBytesTransaction);
    }

    public void createAndOpen(ResizableByteBuffer resizableByteBuffer) throws Exception {
        this.resizableByteBuffer = resizableByteBuffer;

        int size = (2 + 65 - cMinPower) * 8;
        synchronized (headerLock) {
            resizableByteBuffer.execute(size, true, new FilerTransaction<Filer, Void>() {
                @Override
                public Void commit(Filer filer) throws IOException {
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
    }

    public ChunkStore(ResizableByteBuffer resizableByteBuffer) throws Exception {
        this.resizableByteBuffer = resizableByteBuffer;
    }

    public void open() throws IOException {
        synchronized (headerLock) {
            resizableByteBuffer.execute(-1, true, new FilerTransaction<Filer, Void>() {
                @Override
                public Void commit(Filer filer) throws IOException {
                    filer.seek(0);
                    lengthOfFile = FilerIO.readLong(filer, "lengthOfFile");
                    referenceNumber = FilerIO.readLong(filer, "referenceNumber");
                    return null;
                }
            });
        }
    }

    public void delete() throws IOException {
        resizableByteBuffer.delete();
    }

    @Override
    public void copyTo(final ChunkStore to) throws IOException {
        resizableByteBuffer.execute(-1, true, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(final Filer fromFiler) throws IOException {
                to.resizableByteBuffer.execute(fromFiler.length(), true, new FilerTransaction<Filer, Void>() {
                    @Override
                    public Void commit(Filer toFiler) throws IOException {
                        fromFiler.seek(0);
                        toFiler.seek(0);
                        FilerIO.copy(fromFiler, toFiler, -1);
                        return null;
                    }
                });
                return null;
            }
        });
        to.open();
    }

    public long getReferenceNumber() {
        return referenceNumber;
    }

    public void allChunks(final ChunkIdStream chunkIdStream) throws IOException {
        resizableByteBuffer.execute(-1, false, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Filer filer) throws IOException {
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
                return null;
            }
        });
    }

    /**
     *
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
        boolean reused = false;
        long chunkFP;

        // first try to reuse
        synchronized (headerLock) {
            chunkFP = resizableByteBuffer.execute(-1, false, new FilerTransaction<Filer, Long>() {
                @Override
                public Long commit(Filer filer) throws IOException {
                    return reuseChunk(filer, chunkPosition);
                }
            });
            if (chunkFP == -1) {
                final long newChunkFP = lengthOfFile;
                long newSize = newChunkFP + chunkLength;
                resizableByteBuffer.execute(newSize, false, new FilerTransaction<Filer, Void>() {
                    @Override
                    public Void commit(Filer filer) throws IOException {
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
                        return null;
                    }
                });
                chunkFP = newChunkFP;
                reused = true;
            }
        }

        if (reused) {
            reuses[chunkPower].inc(1);
        } else {
            allocates[chunkPower].inc(1);
        }

        final long _chunkFP = chunkFP;
        resizableByteBuffer.execute(-1, false, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Filer filer) throws IOException {
                filer.seek(_chunkFP);
                long magicNumber = FilerIO.readLong(filer, "magicNumber");
                if (magicNumber != cMagicNumber) {
                    throw new IOException("Invalid chunkFP " + _chunkFP);
                }
                int chunkPower = (int) FilerIO.readLong(filer, "chunkPower");
                long nextFreeChunkFP = FilerIO.readLong(filer, "chunkNexFreeChunkFP");
                long length = FilerIO.readLong(filer, "chunkLength");

                long startOfFP = filer.getFilePointer();
                long endOfFP = startOfFP + FilerIO.chunkLength(chunkPower);
                ChunkFiler chunkFiler = new ChunkFiler(ChunkStore.this, filer, _chunkFP, startOfFP, endOfFP);
                chunkFiler.seek(0);
                //TODO if this throws an IOException we should free up the chunkFP
                M monkey = null;
                if (createFiler != null) {
                    monkey = createFiler.create(hint, chunkFiler);
                }
                Chunk<M> chunk = new Chunk<>(monkey, _chunkFP, startOfFP, endOfFP);
                chunk.fp = chunkFiler.getFilePointer();
                openChunks.put(_chunkFP, chunk);
                return null;
            }
        });
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

    public boolean isUsed(final long chunkFP) throws IOException {
        return resizableByteBuffer.execute(-1, false, new FilerTransaction<Filer, Boolean>() {
            @Override
            public Boolean commit(Filer filer) throws IOException {
                filer.seek(chunkFP);
                long magicNumber = FilerIO.readLong(filer, "magicNumber");
                return (magicNumber == cMagicNumber);
            }
        });
    }

    /**
     *
     * @param <M>
     * @param <R>
     * @param chunkFP
     * @param openFiler Nullable
     * @param chunkTransaction
     * @return
     * @throws IOException
     */
    public <M, R> R execute(final long chunkFP, final OpenFiler<M, ChunkFiler> openFiler, final ChunkTransaction<M, R> chunkTransaction) throws IOException {

        @SuppressWarnings("unchecked")
        final Chunk<M> chunk = (Chunk<M>) openChunks.get(chunkFP);
        if (chunk == null) {
            return resizableByteBuffer.execute(-1, false, new FilerTransaction<Filer, R>() {
                @Override
                public R commit(Filer filer) throws IOException {
                    filer.seek(chunkFP);
                    long magicNumber = FilerIO.readLong(filer, "magicNumber");
                    if (magicNumber != cMagicNumber) {
                        throw new IOException("Invalid chunkFP " + chunkFP);
                    }
                    int chunkPower = (int) FilerIO.readLong(filer, "chunkPower");
                    long nextFreeChunkFP = FilerIO.readLong(filer, "chunkNexFreeChunkFP");
                    long length = FilerIO.readLong(filer, "chunkLength");

                    long startOfFP = filer.getFilePointer();
                    long endOfFP = startOfFP + FilerIO.chunkLength(chunkPower);
                    ChunkFiler chunkFiler = new ChunkFiler(ChunkStore.this, filer, chunkFP, startOfFP, endOfFP);
                    chunkFiler.seek(0);

                    M monkey = null;
                    if (openFiler != null) {
                        monkey = openFiler.open(chunkFiler);
                    }
                    Chunk<M> chunk = new Chunk<>(monkey, chunkFP, startOfFP, endOfFP);
                    openChunks.put(chunkFP, chunk);

                    chunkFiler.seek(0);
                    R result = chunkTransaction.commit(chunk.monkey, chunkFiler);
                    chunk.fp = chunkFiler.getFilePointer();

                    return result;
                }
            });
        } else {
            return resizableByteBuffer.execute(-1, false, new FilerTransaction<Filer, R>() {
                @Override
                public R commit(Filer filer) throws IOException {
                    ChunkFiler chunkFiler = new ChunkFiler(ChunkStore.this, filer, chunkFP, chunk.startOfFP, chunk.endOfFP);
                    chunkFiler.seek(chunk.fp);
                    R result = chunkTransaction.commit(chunk.monkey, chunkFiler);
                    chunk.fp = chunkFiler.getFilePointer();
                    return result;
                }
            });
        }
    }

    public void remove(final long _chunkFP) throws IOException {
        final int chunkPower;
        openChunks.remove(_chunkFP);
        synchronized (headerLock) {
            chunkPower = resizableByteBuffer.execute(-1, false, new FilerTransaction<Filer, Integer>() {
                @Override
                public Integer commit(Filer filer) throws IOException {
                    filer.seek(_chunkFP);
                    long magicNumber = FilerIO.readLong(filer, "magicNumber");
                    if (magicNumber != cMagicNumber) {
                        throw new IOException("Invalid chunkFP " + _chunkFP);
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

                    return chunkPower;
                }
            });
        }
        removes[chunkPower].inc(1);
    }

    private long freeSeek(long _chunkPower) {
        return 8 + 8 + ((_chunkPower - cMinPower) * 8);
    }

    private static final byte[] zerosMin = new byte[(int) Math.pow(2, cMinPower)]; // never too big
    private static final byte[] zerosMax = new byte[(int) Math.pow(2, 16)]; // 65536 max used until min needed

    public <M> boolean isValid(final long chunkFP) throws IOException {
        if (openChunks.get(chunkFP) != null) {
            return true;
        }
        return resizableByteBuffer.execute(-1, false, new FilerTransaction<Filer, Boolean>() {
            @Override
            public Boolean commit(Filer filer) throws IOException {
                filer.seek(chunkFP);
                long magicNumber = FilerIO.readLong(filer, "magicNumber");
                return magicNumber == cMagicNumber;
            }
        });
    }

}
