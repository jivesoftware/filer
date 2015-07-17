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
import com.jivesoftware.os.filer.chunk.store.transaction.MapBackedKeyedFPIndex;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCog;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.filer.io.primative.LongKeyMarshaller;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author jonathan.colt
 */
public class TxKeyObjectStoreNGTest {

    TxPartitionedKeyObjectStore<Long, Long> store;

    @BeforeTest
    public void init() throws Exception {
        TxCogs cogs = new TxCogs(256, 64, null, null, null);
        TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> skyhookCog = cogs.getSkyhookCog(0);
        File dir = Files.createTempDirectory("testNewChunkStore").toFile();
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().openOrCreate(new File[] { dir }, 0, "data1", 8, byteBufferFactory, 500, 5_000);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().openOrCreate(new File[] { dir }, 0, "data2", 8, byteBufferFactory, 500, 5_000);

        store = new TxPartitionedKeyObjectStore<>(
            (partitionCount, key) -> Math.abs(key.hashCode() % partitionCount),
            (TxKeyObjectStore<Long, Long>[]) new TxKeyObjectStore[] {
                new TxKeyObjectStore<>(skyhookCog, cogs.getSkyHookKeySemaphores(), 0, chunkStore1, new LongKeyMarshaller(), "booya".getBytes(), 2, 8, false),
                new TxKeyObjectStore<>(skyhookCog, cogs.getSkyHookKeySemaphores(), 0, chunkStore2, new LongKeyMarshaller(), "booya".getBytes(), 2, 8, false)
            });
    }

    @Test
    public void testExecute() throws IOException {
        int numKeys = 16;
        List<Long> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            final long k = i;
            final long v = i;
            keys.add(k);

            store.execute(k, false, context -> {
                Long got = context.get();
                Assert.assertNull(got);
                return null;
            });

            store.execute(k, true, context -> {
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
            });

            store.execute(k, false, context -> {
                Long got = context.get();
                Assert.assertEquals(got, (Long) v);
                return null;
            });
        }

        for (int i = numKeys; i < numKeys * 2; i++) {
            keys.add((long) i);
        }

        boolean[] contains = store.contains(keys);
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
            store.execute(k, true, context -> {
                context.set(v);
                truth.put(k, v);
                return null;
            });
        }

        store.stream((key, value) -> {

            Assert.assertTrue(truth.containsKey(key));
            Long t = truth.remove(key);
            Assert.assertEquals(value, t);
            return true;
        });

        Assert.assertTrue(truth.isEmpty());
    }

    @Test
    public void testStreamKeys() throws Exception {

        final Map<Long, Long> truth = new ConcurrentHashMap<>();
        for (int i = 0; i < 16; i++) {
            final long k = i;
            final long v = i;
            store.execute(k, true, context -> {
                context.set(v);
                truth.put(k, v);
                return null;
            });
        }

        store.streamKeys(key -> {
            Assert.assertTrue(truth.containsKey(key));
            truth.remove(key);
            return true;
        });

        Assert.assertTrue(truth.isEmpty());
    }

}
