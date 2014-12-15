package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.AutoResizingChunkFiler;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class AutoResizingChunkFilerTest {

    @Test(enabled = false)
    public void testConcurrentResizingChunkStore() throws Exception {
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();

        final MultiChunkStore multiChunkStore = new MultiChunkStoreInitializer(new ChunkStoreInitializer())
            .initializeMultiByteBufferBacked("test", byteBufferFactory, 1, 512, true, 8, new ByteArrayStripingLocksProvider(64));

        int numFilers = 20;
        final AutoResizingChunkFiler[] filers = new AutoResizingChunkFiler[numFilers];
        final AtomicInteger[] expectedCounts = new AtomicInteger[numFilers];
        for (int i = 0; i < numFilers; i++) {
            byte[] key = FilerIO.intBytes(i);
            AutoResizingChunkFiler autoResizingChunkFiler = new AutoResizingChunkFiler(multiChunkStore.getTemporaryFilerProvider(key));
            autoResizingChunkFiler.init(32);
            filers[i] = autoResizingChunkFiler;
            expectedCounts[i] = new AtomicInteger();
        }

        final int numIterations = 1_000_000;
        int poolSize = 24;
        final ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        int numRunnables = 24;
        List<Runnable> runnables = new ArrayList<>(numRunnables);

        for (int i = 0; i < numRunnables; i++) {
            final int _i = i;
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    int start = _i * numIterations;
                    for (int j = start; j < start + numIterations; j++) {
                        int filerIndex = j % filers.length;
                        AutoResizingChunkFiler filer = filers[filerIndex];
                        try {
                            synchronized (filer.lock()) {
                                filer.write(FilerIO.intBytes(j));
                                expectedCounts[filerIndex].incrementAndGet();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        if ((j + 1) % 100_000 == 0) {
                            System.out.println("Write iteration " + _i + " -> " + (j + 1));
                        }
                    }
                }
            });
        }

        long t = System.currentTimeMillis();
        List<Future<?>> futures = new ArrayList<>(numRunnables);
        for (Runnable runnable : runnables) {
            futures.add(executor.submit(runnable));
        }
        for (Future<?> future : futures) {
            future.get();
        }
        System.out.println("Finished writing in " + (System.currentTimeMillis() - t));

        for (int i = 0; i < numFilers; i++) {
            Filer filer = filers[i];
            synchronized (filer.lock()) {
                filer.seek(0);
                int expectedNumValues = expectedCounts[i].get();
                Set<Integer> values = new HashSet<>(expectedNumValues);
                for (int j = 0; j < expectedNumValues; j++) {
                    values.add(FilerIO.readInt(filer, "value"));
                }
                assertEquals(values.size(), expectedNumValues);
                System.out.println("Read filer " + i);
            }
        }

    }
}
