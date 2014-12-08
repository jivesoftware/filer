package com.jivesoftware.os.filer.keyed.store;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class FileCursorTest {

    Random rand = new Random(12345);
    int numThreads = 8;
    int numRuns = 100;
    int numreads = 100000;

    int legnth = 1024 * 1024 * 10;
    RandomAccessFile raf;
    ExecutorService executorService;

    @BeforeMethod
    public void setUp() throws Exception {
        executorService = Executors.newFixedThreadPool(numThreads);

        File directory = Files.createTempDir();
        File file = new File(directory, "baseline");
        raf = new RandomAccessFile(file, "rw");
        raf.seek(legnth);
        raf.write(0);
        raf.seek(0);

        byte[] randomBytes = new byte[1024];
        for (int i = 0; i < legnth / 1024; i++) {
            rand.nextBytes(randomBytes);
            raf.write(randomBytes);
        }
    }

    @AfterMethod
    public void tearDown() throws IOException {
        raf.close();
        executorService.shutdownNow();
    }

    @Test(enabled = false)
    public void randomReadsBaseline() throws Exception {

        final Object uberLock = new Object();
        List<Future> await = new ArrayList<>();

        mmemapped:
        {
            long minElaspe = Long.MAX_VALUE;
            for (int r = 0; r < numRuns; r++) {
                long start = System.currentTimeMillis();
                FileChannel fileChannel = raf.getChannel();
                final MappedByteBuffer channel = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (int) fileChannel.size());
                for (int i = 0; i < numThreads; i++) {
                    await.add(executorService.submit(new Runnable() {

                        @Override
                        public void run() {
                            for (int r = 0; r < numreads; r++) {
                                synchronized (uberLock) {
                                    try {
                                        channel.get(rand.nextInt(legnth));
                                    } catch (Exception x) {
                                        x.printStackTrace();
                                    }
                                }
                            }
                        }
                    }));
                }

                for (Future a : await) {
                    a.get();
                }

                minElaspe = Math.min(minElaspe, System.currentTimeMillis() - start);
            }
            System.out.println("mem-mapped:" + minElaspe);
        }
        await.clear();

        mmemapped_duplicate:
        {
            long minElaspe = Long.MAX_VALUE;
            for (int r = 0; r < numRuns; r++) {
                long start = System.currentTimeMillis();
                FileChannel fileChannel = raf.getChannel();
                final MappedByteBuffer channel = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (int) fileChannel.size());
                final ByteBuffer bb = channel.duplicate();
                for (int i = 0; i < numThreads; i++) {
                    await.add(executorService.submit(new Runnable() {

                        @Override
                        public void run() {
                            for (int r = 0; r < numreads; r++) {
                                try {
                                    bb.get(rand.nextInt(legnth));
                                } catch (Exception x) {
                                    x.printStackTrace();
                                }
                            }
                        }
                    }));
                }

                for (Future a : await) {
                    a.get();
                }

                minElaspe = Math.min(minElaspe, System.currentTimeMillis() - start);
            }
            System.out.println("mmemapped_duplicate:" + minElaspe);
        }
        await.clear();

        mmemapped_descrete_channels:
        {
            long minElaspe = Long.MAX_VALUE;
            for (int r = 0; r < numRuns; r++) {
                long start = System.currentTimeMillis();

                for (int i = 0; i < numThreads; i++) {
                    final FileChannel fileChannel = raf.getChannel();
                    final MappedByteBuffer channel = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (int) fileChannel.size());
                    await.add(executorService.submit(new Runnable() {

                        @Override
                        public void run() {
                            for (int r = 0; r < numreads; r++) {
                                try {
                                    channel.get(rand.nextInt(legnth));
                                } catch (Exception x) {
                                    x.printStackTrace();
                                }
                            }
                        }
                    }));

                }

                for (Future a : await) {
                    a.get();
                }

                minElaspe = Math.min(minElaspe, System.currentTimeMillis() - start);
            }
            System.out.println("mmemapped_descrete_channels:" + minElaspe);

        }

        await.clear();

        base:
        {
            long minElaspe = Long.MAX_VALUE;
            for (int r = 0; r < numRuns; r++) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < numThreads; i++) {
                    await.add(executorService.submit(new Runnable() {

                        @Override
                        public void run() {
                            for (int r = 0; r < numreads; r++) {
                                synchronized (uberLock) {
                                    try {
                                        raf.seek(rand.nextInt(legnth));
                                        raf.read();
                                    } catch (Exception x) {
                                        x.printStackTrace();
                                    }
                                }
                            }
                        }
                    }));
                }

                for (Future a : await) {
                    a.get();
                }

                minElaspe = Math.min(minElaspe, System.currentTimeMillis() - start);
            }
            System.out.println("mem-mapped:" + minElaspe);
        }
        await.clear();

    }
}
