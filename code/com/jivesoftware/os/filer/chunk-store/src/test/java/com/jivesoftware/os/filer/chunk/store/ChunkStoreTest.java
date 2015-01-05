/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author jonathan.colt
 */
public class ChunkStoreTest {

    private final NoOpOpenFiler<ChunkFiler> openFiler = new NoOpOpenFiler<>();
    private final NoOpCreateFiler<ChunkFiler> createFiler = new NoOpCreateFiler<>();

    @Test(enabled = false)
    public void testChunkStorePerformance() throws Exception {
        final File chunkFile = File.createTempFile("chunk", "test");
        final int numThreads = 16;
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        final int numLoops = 100;
        final int numIterations = 100_000;
        final long filerSize = 200_000_000;
        final long chunkLength = 1_000;

        for (int loop = 0; loop < numLoops; loop++) {
            chunkFile.createNewFile();

            final ChunkStore chunkStore = new ChunkStoreInitializer().create(
                new ByteBufferProvider(
                    chunkFile.getName().getBytes(),
                    new FileBackedMemMappedByteBufferFactory(chunkFile.getParentFile())),
                filerSize,
                true,
                8);

            long start = System.currentTimeMillis();
            List<Future<?>> futures = new ArrayList<>(numThreads);
            for (int n = 0; n < numThreads; n++) {
                futures.add(executorService.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        for (int i = 0; i < numIterations / numThreads; i++) {
                            long chunkFP = chunkStore.newChunk(chunkLength, createFiler);
                        }
                        return null;
                    }
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
        String[] chunkPaths = new String[] { Files.createTempDirectory("testNewChunkStore").toFile().getAbsolutePath() };
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunkPaths, "data", 0, size, false, 8);

        long chunk10 = chunkStore.newChunk(10L, createFiler);
        System.out.println("chunkId:" + chunk10);
        writeIntToChunk(chunkStore, chunk10, 10);
        assertIntInChunk(chunkStore, chunk10, 10);
    }

    private void writeIntToChunk(ChunkStore chunkStore, long chunkFP, final int value) throws IOException {
        chunkStore.execute(chunkFP, openFiler, new ChunkTransaction<Void, Void>() {
            @Override
            public Void commit(Void monkey, ChunkFiler filer) throws IOException {
                FilerIO.writeInt(filer, value, "");
                return null;
            }
        });
    }

    private void assertIntInChunk(ChunkStore chunkStore, long chunk10, final int expected) throws IOException {
        chunkStore.execute(chunk10, openFiler, new ChunkTransaction<Void, Void>() {
            @Override
            public Void commit(Void monkey, ChunkFiler filer) throws IOException {
                filer.seek(0);
                int value = FilerIO.readInt(filer, "");
                System.out.println("expected:" + value);
                assertEquals(value, expected);
                return null;
            }
        });
    }

    @Test
    public void testExistingChunkStore() throws Exception {
        int size = 1024 * 10;
        String[] chunkPaths = new String[] { Files.createTempDirectory("testExistingChunkStore").toFile().getAbsolutePath() };
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunkPaths, "data", 0, size, false, 8);

        long chunk10 = chunkStore.newChunk(10L, createFiler);
        writeIntToChunk(chunkStore, chunk10, 10);

        long expectedReferenceNumber = chunkStore.getReferenceNumber();

        chunkStore = new ChunkStoreInitializer().initialize(chunkPaths, "data", 0, size, false, 8);
        assertEquals(chunkStore.getReferenceNumber(), expectedReferenceNumber);

        assertIntInChunk(chunkStore, chunk10, 10);
    }

    @Test
    public void testResizingChunkStore() throws Exception {
        final int size = 512;
        String[] chunkPaths = new String[] { Files.createTempDirectory("testResizingChunkStore").toFile().getAbsolutePath() };
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunkPaths, "data", 0, size, true, 8);

        long chunk10 = chunkStore.newChunk(size * 4L, createFiler);
        chunkStore.execute(chunk10, openFiler, new ChunkTransaction<Void, Void>() {
            @Override
            public Void commit(Void monkey, ChunkFiler filer) throws IOException {
                byte[] bytes = new byte[size * 4];
                bytes[0] = 1;
                bytes[bytes.length - 1] = 1;
                FilerIO.write(filer, bytes);
                return null;
            }
        });

        chunkStore.execute(chunk10, openFiler, new ChunkTransaction<Void, Void>() {
            @Override
            public Void commit(Void monkey, ChunkFiler filer) throws IOException {
                filer.seek(0);
                byte[] bytes = new byte[size * 4];
                FilerIO.read(filer, bytes);
                assertEquals(bytes[0], 1);
                assertEquals(bytes[bytes.length - 1], 1);
                return null;
            }
        });
    }

    @Test
    public void testAddRemove() throws Exception {
        String[] chunkPaths = new String[] { Files.createTempDirectory("testAddRemove").toFile().getAbsolutePath() };
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunkPaths, "test", 0, 512, true, 8);

        List<Long> fps1 = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            fps1.add(chunkStore.newChunk(1024L, createFiler));
        }

        for (long fp : fps1) {
            chunkStore.remove(fp);
        }

        List<Long> fps2 = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            fps2.add(chunkStore.newChunk(1024L, createFiler));
        }

        System.out.println(fps1);
        System.out.println(fps2);
        Collections.sort(fps1);
        Collections.sort(fps2);

        assertEquals(fps1, fps2);
    }
}
