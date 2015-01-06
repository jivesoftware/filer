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

import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.PartitionFunction;
import com.jivesoftware.os.filer.io.primative.LongKeyMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class TxKeyObjectStoreNGTest {

    TxPartitionedKeyObjectStore<Long, Long> store;

    @BeforeTest
    public void init() throws IOException, Exception {
        String[] chunkPaths = new String[]{Files.createTempDirectory("testNewChunkStore")
            .toFile()
            .getAbsolutePath()};
        ChunkStore chunkStore1 = new ChunkStoreInitializer().initialize(chunkPaths, "data1", 0, 10, true, 8);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().initialize(chunkPaths, "data2", 0, 10, true, 8);

        store = new TxPartitionedKeyObjectStore<>(new PartitionFunction<Long>() {

            @Override
            public int partition(int partitionCount, Long key) {
                return Math.abs(key.hashCode()) % partitionCount;
            }
        }, new TxKeyObjectStore[]{
            new TxKeyObjectStore<>(chunkStore1, new LongKeyMarshaller(), "booya".getBytes(), 2, 8, false),
            new TxKeyObjectStore<>(chunkStore2, new LongKeyMarshaller(), "booya".getBytes(), 2, 8, false)
        }
        );
    }

    @Test
    public void testExecute() throws IOException {
        for (int i = 0; i < 16; i++) {
            final long k = i;
            final long v = i;

            store.execute(k, false, new KeyValueTransaction<Long, Void>() {

                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    Long got = context.get();
                    Assert.assertNull(got);
                    return null;
                }
            });

            store.execute(k, true, new KeyValueTransaction<Long, Void>() {

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

            store.execute(k, false, new KeyValueTransaction<Long, Void>() {

                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    Long got = context.get();
                    Assert.assertEquals(got, (Long) v);
                    return null;
                }
            });
        }

    }

    @Test
    public void testStream() throws Exception {

        final Map<Long, Long> truth = new ConcurrentHashMap<>();
        for (int i = 0; i < 16; i++) {
            final long k = i;
            final long v = i;
            store.execute(k, true, new KeyValueTransaction<Long, Void>() {

                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    context.set(v);
                    truth.put(k, v);
                    return null;
                }
            });
        }

        store.stream(new KeyValueStore.EntryStream<Long, Long>() {

            @Override
            public boolean stream(Long key, Long value) throws IOException {

                Assert.assertTrue(truth.containsKey(key));
                Long t = truth.remove(key);
                Assert.assertEquals(value, t);
                return true;
            }
        });

        Assert.assertTrue(truth.isEmpty());
    }

    @Test
    public void testStreamKeys() throws Exception {

        final Map<Long, Long> truth = new ConcurrentHashMap<>();
        for (int i = 0; i < 16; i++) {
            final long k = i;
            final long v = i;
            store.execute(k, true, new KeyValueTransaction<Long, Void>() {

                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    context.set(v);
                    truth.put(k, v);
                    return null;
                }
            });
        }

        store.streamKeys(new KeyValueStore.KeyStream<Long>() {

            @Override
            public boolean stream(Long key) throws IOException {
                Assert.assertTrue(truth.containsKey(key));
                truth.remove(key);
                return true;
            }

        });

        Assert.assertTrue(truth.isEmpty());
    }

}
