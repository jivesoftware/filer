/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMapOfFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
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
        TxCogs cogs = new TxCogs(256, 64, null, null, null);

        Random rand = new Random(1234);
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory bbf = new HeapByteBufferFactory();
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().create(bbf, 8, byteBufferFactory, 500, 5_000, stackBuffer);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().create(bbf, 8, byteBufferFactory, 500, 5_000, stackBuffer);
        final ChunkStore[] chunkStores = new ChunkStore[]{chunkStore1, chunkStore2};

        int numThread = 24;
        ConcurrentHashMap<Long, TxKeyedFilerStore> namedStores = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(numThread);
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < 100; i++) {
            executor.execute(new ConcurrencyRunnable(cogs, rand, namedStores, chunkStores, stop));
        }
        Thread.sleep(60_000);
        stop.set(true);
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        System.out.println("Done.");
    }

    private class ConcurrencyRunnable implements Runnable {

        TxCogs cogs;
        Random rand;
        ConcurrentHashMap<Long, TxKeyedFilerStore> namedStores;
        ChunkStore[] chunkStores;
        AtomicBoolean stop;
        StackBuffer stackBuffer = new StackBuffer();

        ConcurrencyRunnable(TxCogs cogs, Random rand, ConcurrentHashMap<Long, TxKeyedFilerStore> namedStores, ChunkStore[] chunkStores, AtomicBoolean stop) {
            this.cogs = cogs;
            this.rand = rand;
            this.stop = stop;
            this.namedStores = namedStores;
            this.chunkStores = chunkStores;
        }

        TxKeyedFilerStore get(long name) {
            TxKeyedFilerStore got = namedStores.get(name);
            if (got == null) {
                got = new TxKeyedFilerStore(cogs,
                    0,
                    chunkStores,
                    FilerIO.longBytes(name),
                    false,
                    TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
                    TxNamedMapOfFiler.CHUNK_FILER_OPENER,
                    TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
                    TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);
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
                    store.read(key, -1, (monkey, filer, stackBuffer, lock) -> {
                        if (filer != null) {
                            synchronized (lock) {
                                filer.seek(0);
                                for (int i = 0; i < filer.length(); i++) {
                                    filer.read();
                                }
                            }
                        }
                        return true;
                    }, stackBuffer);

                    // write
                    final long filerLength = 1 + rand.nextInt(1024);
                    store.readWriteAutoGrow(key, filerLength, (monkey, filer, stackBuffer, lock) -> {
                        synchronized (lock) {
                            filer.seek(0);
                            Assert.assertFalse(filer.length() < filerLength, "Was:" + filer.length() + " expected: " + filerLength);
                            for (int i = 0; i < filerLength; i++) {
                                filer.write((byte) rand.nextInt(255));
                            }
                            return true;
                        }
                    }, stackBuffer);

                    // rewrite
                    final long rewriteFilerLength = 1 + rand.nextInt(1024);
                    store.writeNewReplace(key, rewriteFilerLength, (monkey, newFiler, stackBuffer, newLock) -> {
                        synchronized (newLock) {
                            newFiler.seek(0);
                            for (int i = 0; i < rewriteFilerLength; i++) {
                                newFiler.write((byte) rand.nextInt(255));
                            }
                            return true;
                        }
                    }, stackBuffer);

                } catch (Exception x) {
                    x.printStackTrace();
                }
                System.out.println(".");
            }

            System.out.println("Thread " + Thread.currentThread() + " done.");
        }
    }
}
