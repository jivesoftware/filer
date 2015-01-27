/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class TxKeyedFilerStoreConcurrencyNGTest {

    @Test(enabled = false)
    public void concurrencyTest() throws Exception {

        Random rand = new Random(1234);

        ByteBufferFactory bbf = new HeapByteBufferFactory();
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().create(bbf, 8, byteBufferFactory, 5_000);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().create(bbf, 8, byteBufferFactory, 5_000);
        final ChunkStore[] chunkStores = new ChunkStore[]{chunkStore1, chunkStore2};

        int numThread = 24;
        ConcurrentHashMap<Long, TxKeyedFilerStore> namedStores = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(numThread);
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < 100; i++) {
            executor.execute(new ConcurrencyRunnable(rand, namedStores, chunkStores, stop));
        }
        Thread.sleep(60_000);
        stop.set(true);
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        System.out.println("Done.");
    }

    private class ConcurrencyRunnable implements Runnable {

        Random rand;
        ConcurrentHashMap<Long, TxKeyedFilerStore> namedStores;
        ChunkStore[] chunkStores;
        AtomicBoolean stop;

        ConcurrencyRunnable(Random rand, ConcurrentHashMap<Long, TxKeyedFilerStore> namedStores, ChunkStore[] chunkStores, AtomicBoolean stop) {
            this.rand = rand;
            this.stop = stop;
            this.namedStores = namedStores;
            this.chunkStores = chunkStores;
        }

        TxKeyedFilerStore get(long name) {
            TxKeyedFilerStore got = namedStores.get(name);
            if (got == null) {
                got = new TxKeyedFilerStore(chunkStores, FilerIO.longBytes(name), false);
                TxKeyedFilerStore had = namedStores.putIfAbsent(name, got);
                if (had != null) {
                    got = had;
                }
            }
            return got;
        }

        @Override
        public void run() {

            while (!stop.get()) {
                try {
                    TxKeyedFilerStore store = get(rand.nextInt(2));
                    byte[] key = FilerIO.longBytes(rand.nextInt(256));
                    // read
                    store.execute(key, -1, new FilerTransaction<Filer, Boolean>() {

                        @Override
                        public Boolean commit(Object lock, Filer filer) throws IOException {
                            if (filer != null) {
                                synchronized (lock) {
                                    filer.seek(0);
                                    for (int i = 0; i < filer.length(); i++) {
                                        filer.read();
                                    }
                                }
                            }
                            return true;
                        }
                    });

                    // write
                    final long filerLength = 1 + rand.nextInt(1024);
                    store.execute(key, filerLength, new FilerTransaction<Filer, Boolean>() {

                        @Override
                        public Boolean commit(Object lock, Filer filer) throws IOException {
                            synchronized (lock) {
                                filer.seek(0);
                                Assert.assertFalse(filer.length() < filerLength, "Was:" + filer.length() + " expected: " + filerLength);
                                for (int i = 0; i < filerLength; i++) {
                                    filer.write((byte) rand.nextInt(255));
                                }
                                return true;
                            }
                        }
                    });

                    // rewrite
                    final long rewriteFilerLength = 1 + rand.nextInt(1024);
                    store.executeRewrite(key, rewriteFilerLength, new RewriteFilerTransaction<Filer, Boolean>() {

                        @Override
                        public Boolean commit(Filer currentFiler, Filer newFiler) throws IOException {
                            if (currentFiler != null) {
                                currentFiler.seek(0);
                                for (int i = 0; i < currentFiler.length(); i++) {
                                    currentFiler.read();
                                }
                            }
                            newFiler.seek(0);
                            for (int i = 0; i < rewriteFilerLength; i++) {
                                newFiler.write((byte) rand.nextInt(255));
                            }
                            return true;
                        }
                    });

                } catch (Exception x) {
                    x.printStackTrace();
                }
            }

            System.out.println("Thread " + Thread.currentThread() + " done.");
        }
    }
}
