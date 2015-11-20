/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author jonathan.colt
 */
public class ChunkStoreTest {

    private final NoOpOpenFiler<ChunkFiler> openFiler = new NoOpOpenFiler<>();
    private final NoOpCreateFiler<ChunkFiler> createFiler = new NoOpCreateFiler<>();

    @Test(enabled = false)
    public void testChunkStorePerformance() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        final File chunkFile = File.createTempFile("chunk", "test");
        final int numThreads = 16;
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();

        final int numLoops = 100;
        final int numIterations = 100_000;
        final long chunkLength = 1_000;

        for (int loop = 0; loop < numLoops; loop++) {
            chunkFile.createNewFile();

            File[] dirs = {chunkFile.getParentFile()};
            final ChunkStore chunkStore = new ChunkStoreInitializer().openOrCreate(dirs, 0, "data", 1024, byteBufferFactory, 500, 5_000, primitiveBuffer);

            long start = System.currentTimeMillis();
            List<Future<?>> futures = new ArrayList<>(numThreads);
            for (int n = 0; n < numThreads; n++) {
                futures.add(executorService.submit(() -> {
                    for (int i = 0; i < numIterations / numThreads; i++) {
                        long chunkFP = chunkStore.newChunk(chunkLength, createFiler, primitiveBuffer);
                    }
                    return null;
                }));
            }

            for (Future<?> future : futures) {
                future.get();
            }

            System.out.println("Finished in " + (System.currentTimeMillis() - start));

            chunkFile.delete();
        }
    }

    @Test
    public void testNewChunkStore() throws Exception {
        int size = 1024 * 10;
        byte[] primitiveBuffer = new byte[8];

        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        File[] dirs = {Files.createTempDirectory("testNewChunkStore").toFile()};
        ChunkStore chunkStore = new ChunkStoreInitializer().openOrCreate(dirs, 0, "data", size, byteBufferFactory, 500, 5_000, primitiveBuffer);

        long chunk10 = chunkStore.newChunk(10L, createFiler, primitiveBuffer);
        System.out.println("chunkId:" + chunk10);
        writeIntToChunk(chunkStore, chunk10, 10, primitiveBuffer);
        assertIntInChunk(chunkStore, chunk10, 10, primitiveBuffer);
    }

    @Test
    public void testCheckExists() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        File[] dirs = {
            Files.createTempDirectory("testChunkStoreExists").toFile(),
            Files.createTempDirectory("testChunkStoreExists").toFile(),
            Files.createTempDirectory("testChunkStoreExists").toFile()
        };

        ChunkStore[] chunkStores = new ChunkStore[3];
        long[] fps = new long[chunkStores.length];
        for (int i = 0; i < chunkStores.length; i++) {
            chunkStores[i] = new ChunkStoreInitializer().openOrCreate(dirs, i, "data", 4_096, byteBufferFactory, 500, 5_000, primitiveBuffer);
            fps[i] = chunkStores[i].newChunk(1024L, new CreateFiler<Long, Void, ChunkFiler>() {
                @Override
                public long sizeInBytes(Long hint) throws IOException {
                    return hint;
                }

                @Override
                public Void create(Long hint, ChunkFiler filer, byte[] primitiveBuffer) throws IOException {
                    return null;
                }
            }, primitiveBuffer);
        }

        for (int i = 0; i < chunkStores.length; i++) {
            assertTrue(new ChunkStoreInitializer().checkExists(dirs, i, "data"));
            assertTrue(new ChunkStoreInitializer().openOrCreate(dirs, i, "data", 4_096, byteBufferFactory, 500, 5_000, primitiveBuffer).isValid(fps[i],
                primitiveBuffer));
        }
    }

    private void writeIntToChunk(ChunkStore chunkStore, long chunkFP, final int value, byte[] primitiveBuffer) throws IOException {
        chunkStore.execute(chunkFP, openFiler, (monkey, filer, primitiveBuffer1, lock) -> {
            synchronized (lock) {
                FilerIO.writeInt(filer, value, "", primitiveBuffer1);
                return null;
            }
        }, primitiveBuffer);
    }

    private void assertIntInChunk(ChunkStore chunkStore, long chunk10, final int expected, byte[] primitiveBuffer) throws IOException {
        chunkStore.execute(chunk10, openFiler, (monkey, filer, primitiveBuffer1, lock) -> {
            synchronized (lock) {
                int value = FilerIO.readInt(filer, "", primitiveBuffer1);
                System.out.println("expected:" + value);
                assertEquals(value, expected);
                return null;
            }
        }, primitiveBuffer);
    }

    @Test
    public void testBoundaryCase() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        int size = 1024 * 10;
        File dir = Files.createTempDirectory("testExistingChunkStore").toFile();
        StripingLocksProvider<Long> locksProvider = new StripingLocksProvider<>(64);
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data", size, byteBufferFactory, 500, 5_000, primitiveBuffer);

        final byte[] bytes = new byte[257];
        new Random().nextBytes(bytes);

        for (int i = 0; i < 1_000; i++) {
            long chunk257 = chunkStore.newChunk(257L, createFiler, primitiveBuffer);
            chunkStore.execute(chunk257, new NoOpOpenFiler<>(), (Void monkey, ChunkFiler filer, byte[] primitiveBuffer1, Object lock) -> {
                synchronized (lock) {
                    filer.seek(0);
                    filer.write(bytes);
                }
                return null;
            }, primitiveBuffer);
            chunkStore.execute(chunk257, new NoOpOpenFiler<>(), (Void monkey, ChunkFiler filer, byte[] primitiveBuffer1, Object lock) -> {
                synchronized (lock) {
                    filer.seek(0);
                    byte[] actual = new byte[257];
                    filer.read(actual);
                    assertEquals(actual, bytes);
                }
                return null;
            }, primitiveBuffer);
        }
    }

    @Test
    public void testExistingChunkStore() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        int size = 1024 * 10;
        File dir = Files.createTempDirectory("testExistingChunkStore").toFile();
        StripingLocksProvider<Long> locksProvider = new StripingLocksProvider<>(64);
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data", size, byteBufferFactory, 500, 5_000, primitiveBuffer);

        long chunk10 = chunkStore.newChunk(10L, createFiler, primitiveBuffer);
        writeIntToChunk(chunkStore, chunk10, 10, primitiveBuffer);

        long expectedReferenceNumber = chunkStore.getReferenceNumber();

        chunkStore = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data", 1024, byteBufferFactory, 500, 5_000, primitiveBuffer);
        assertEquals(chunkStore.getReferenceNumber(), expectedReferenceNumber);

        assertIntInChunk(chunkStore, chunk10, 10, primitiveBuffer);
    }

    @Test
    public void testResizingChunkStore() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        final int size = 512;
        File dir = Files.createTempDirectory("testResizingChunkStore").toFile();
        StripingLocksProvider<Long> locksProvider = new StripingLocksProvider<>(64);
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data", size, byteBufferFactory, 500, 5_000, primitiveBuffer);

        long chunk10 = chunkStore.newChunk(size * 4L, createFiler, primitiveBuffer);
        chunkStore.execute(chunk10, openFiler, (monkey, filer, primitiveBuffer1, lock) -> {
            synchronized (lock) {
                byte[] bytes = new byte[size * 4];
                bytes[0] = 1;
                bytes[bytes.length - 1] = 1;
                FilerIO.write(filer, bytes);
                return null;
            }
        }, primitiveBuffer);

        chunkStore.execute(chunk10, openFiler, (monkey, filer, primitiveBuffer1, lock) -> {
            synchronized (lock) {
                byte[] bytes = new byte[size * 4];
                FilerIO.read(filer, bytes);
                assertEquals(bytes[0], 1);
                assertEquals(bytes[bytes.length - 1], 1);
                return null;
            }
        }, primitiveBuffer);
    }

    @Test
    public void testAddRemove() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        File dir = Files.createTempDirectory("testAddRemove").toFile();
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data", 1024, byteBufferFactory, 500, 5_000, primitiveBuffer);

        List<Long> fps1 = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            fps1.add(chunkStore.newChunk(1024L, createFiler, primitiveBuffer));
        }

        for (long fp : fps1) {
            chunkStore.remove(fp, primitiveBuffer);
        }

        List<Long> fps2 = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            fps2.add(chunkStore.newChunk(1024L, createFiler, primitiveBuffer));
        }

        System.out.println(fps1);
        System.out.println(fps2);
        Collections.sort(fps1);
        Collections.sort(fps2);

        assertEquals(fps1, fps2);
    }
}
