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
package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.api.KeyValueContext;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.api.KeyValueTransaction;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.primative.LongLongKeyValueMarshaller;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author jonathan.colt
 */
public class TxKeyValueStoreNGTest {

    Random rand = new Random(1234);
    TxKeyValueStore<Long, Long> store1;
    TxKeyValueStore<Long, Long> store2;

    @BeforeMethod
    public void init() throws Exception {
        ByteBufferFactory bbf = new HeapByteBufferFactory();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().create(bbf, 8, bbf, 5_000);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().create(bbf, 8, bbf, 5_000);
        ChunkStore[] chunkStores = new ChunkStore[] { chunkStore1, chunkStore2 };

        store1 = new TxKeyValueStore<>(chunkStores, new LongLongKeyValueMarshaller(), "booya1".getBytes(), 8, false, 8, false);
        store2 = new TxKeyValueStore<>(chunkStores, new LongLongKeyValueMarshaller(), "booya2".getBytes(), 8, false, 8, false);
    }

    @Test(enabled = false)
    public void testConcurrency() throws IOException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(24);
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < 100; i++) {
            List<Long> keys = new ArrayList<>();
            int keyCount = 5 + rand.nextInt(10);
            for (int j = 0; j < keyCount; j++) {
                keys.add((long) rand.nextInt(5));
            }
            executor.execute(new ConcurrencyRunnable(stop, keys));
        }
        Thread.sleep(30_000);
        stop.set(true);
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        System.out.println("Done.");
    }

    private class ConcurrencyRunnable implements Runnable {

        AtomicBoolean stop;
        List<Long> keys;

        ConcurrencyRunnable(AtomicBoolean stop, List<Long> keys) {
            this.stop = stop;
            this.keys = keys;
        }

        @Override
        public void run() {

            while (!stop.get()) {
                try {
                    TxKeyValueStore<Long, Long> store = rand.nextBoolean() ? store1 : store2;
                    Long key = keys.get(rand.nextInt(keys.size()));
                    Boolean got = store.execute(key, false, new KeyValueTransaction<Long, Boolean>() {

                        @Override
                        public Boolean commit(KeyValueContext<Long> context) throws IOException {
                            return context.get() != null;
                        }
                    });

                    store.execute(key, true, new KeyValueTransaction<Long, Boolean>() {

                        @Override
                        public Boolean commit(KeyValueContext<Long> context) throws IOException {
                            context.set(rand.nextLong());
                            return true;
                        }
                    });
                } catch (IOException x) {
                    x.printStackTrace();
                }
            }

            System.out.println("Thread " + Thread.currentThread() + " done.");
        }

    }

    @Test
    public void testExecute() throws IOException {
        int numKeys = 16;
        List<Long> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            final long k = i;
            final long v = i;
            keys.add(k);
            System.out.println(k + " " + v);

            store1.execute(k, false, new KeyValueTransaction<Long, Void>() {

                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    Long got = context.get();
                    Assert.assertNull(got);
                    return null;
                }
            });

            store1.execute(k, true, new KeyValueTransaction<Long, Void>() {

                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    Long got = context.get();
                    Assert.assertNull(got);
                    context.set(v);
                    got = context.get();
                    Assert.assertNotNull(got);
                    context.remove();
                    got = context.get();
                    Assert.assertNull(got);
                    context.set(v);
                    return null;
                }
            });

            store1.execute(k, false, new KeyValueTransaction<Long, Void>() {

                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    Long got = context.get();
                    Assert.assertEquals(got, (Long) v);
                    return null;
                }
            });
        }
        for (int i = numKeys; i < numKeys * 2; i++) {
            keys.add((long) i);
        }

        boolean[] contains = store1.contains(keys);
        for (int i = 0; i < numKeys * 2; i++) {
            if (i < numKeys) {
                assertTrue(contains[i]);
            } else {
                assertFalse(contains[i]);
            }
        }
    }

    @Test
    public void testStream() throws Exception {

        final Map<Long, Long> truth = new ConcurrentHashMap<>();
        for (int i = 0; i < 16; i++) {
            final long k = i;
            final long v = i;
            store1.execute(k, true, new KeyValueTransaction<Long, Void>() {

                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    context.set(v);
                    truth.put(k, v);
                    return null;
                }
            });
        }

        store1.stream(new KeyValueStore.EntryStream<Long, Long>() {

            @Override
            public boolean stream(Long key, Long value) throws IOException {

                assertTrue(truth.containsKey(key));
                Long t = truth.remove(key);
                Assert.assertEquals(value, t);
                return true;
            }
        });

        assertTrue(truth.isEmpty());
    }

    @Test
    public void testStreamKeys() throws Exception {

        final Map<Long, Long> truth = new ConcurrentHashMap<>();
        for (int i = 0; i < 16; i++) {
            final long k = i;
            final long v = i;
            store1.execute(k, true, new KeyValueTransaction<Long, Void>() {

                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    context.set(v);
                    truth.put(k, v);
                    return null;
                }
            });
        }

        store1.streamKeys(new KeyValueStore.KeyStream<Long>() {

            @Override
            public boolean stream(Long key) throws IOException {
                assertTrue(truth.containsKey(key));
                truth.remove(key);
                return true;
            }

        });

        assertTrue(truth.isEmpty());
    }

}
