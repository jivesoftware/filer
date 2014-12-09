/*
 * Copyright 2014 Jive Software.
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
package com.jivesoftware.os.filer.chunk.store;

import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.ConcurrentFilerProviderBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.PartitionedMapChunkBackedMapStore;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Random;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 * @author jonathan.colt
 */
public class MultiChunkStoreTest {

    @Test
    public void testAllocateAndGet_heap() throws Exception {

        //Files.createTempDirectory("map").toFile().getAbsolutePath()
        MultiChunkStoreInitializer mcsi = new MultiChunkStoreInitializer(new ChunkStoreInitializer());
        MultiChunkStoreConcurrentFilerFactory store = mcsi.initializeMultiByteBufferBacked("boo", new HeapByteBufferFactory(), 10, 512, true, 10, 10);

        long seed = 12345;
        writeRandom(store, seed);
        readAndAssertRandom(store, seed);
    }

    @Test
    public void testAllocateAndGet_fileBacked() throws Exception {

        File mapDir = Files.createTempDirectory("map").toFile();
        MultiChunkStoreInitializer mcsi = new MultiChunkStoreInitializer(new ChunkStoreInitializer());
        FileBackedMemMappedByteBufferFactory byteBufferFactory = new FileBackedMemMappedByteBufferFactory(mapDir);
        MultiChunkStoreConcurrentFilerFactory store = mcsi.initializeMultiByteBufferBacked("boo", byteBufferFactory, 10, 512, true, 10, 10);

        long seed = 12345;
        writeRandom(store, seed);

        byteBufferFactory = new FileBackedMemMappedByteBufferFactory(mapDir);
        store = mcsi.initializeMultiByteBufferBacked("boo", byteBufferFactory, 10, 512, true, 10, 10);

        readAndAssertRandom(store, seed);
    }

    @Test
    public void testMapStore_heap() throws Exception {
        MultiChunkStoreInitializer mcsi = new MultiChunkStoreInitializer(new ChunkStoreInitializer());
        MultiChunkStoreConcurrentFilerFactory store = mcsi.initializeMultiByteBufferBacked("boo", new HeapByteBufferFactory(), 10, 512, true, 10, 10);

        ConcurrentFilerProviderBackedMapChunkFactory<ChunkFiler> factory = new ConcurrentFilerProviderBackedMapChunkFactory<>(8, false, 4, false, 16,
            new ConcurrentFilerProvider<>("booya".getBytes(), store));


        PartitionedMapChunkBackedMapStore<ChunkFiler, Long, Integer> mapStore = new PartitionedMapChunkBackedMapStore<>(factory,
            new StripingLocksProvider<String>(8),
            null,
            zeroPartitioner,
            longIntegerMarshaller);

        for (int i = 0; i < 1_000; i++) {
            mapStore.add((long) i, i);
        }

        for (int i = 0; i < 1_000; i++) {
            assertEquals(mapStore.get((long) i).intValue(), i);
        }
    }

    @Test
    public void testMapStore_fileBacked() throws Exception {
        File mapDir = Files.createTempDirectory("map").toFile();
        MultiChunkStoreInitializer mcsi = new MultiChunkStoreInitializer(new ChunkStoreInitializer());
        FileBackedMemMappedByteBufferFactory byteBufferFactory = new FileBackedMemMappedByteBufferFactory(mapDir);
        MultiChunkStoreConcurrentFilerFactory store = mcsi.initializeMultiByteBufferBacked("boo", byteBufferFactory, 10, 512, true, 10, 10);

        ConcurrentFilerProviderBackedMapChunkFactory<ChunkFiler> factory = new ConcurrentFilerProviderBackedMapChunkFactory<>(8, false, 4, false, 16,
            new ConcurrentFilerProvider<>("booya".getBytes(), store));

        PartitionedMapChunkBackedMapStore<ChunkFiler, Long, Integer> mapStore = new PartitionedMapChunkBackedMapStore<>(factory,
            new StripingLocksProvider<String>(8),
            null,
            zeroPartitioner,
            longIntegerMarshaller);

        for (int i = 0; i < 1_000; i++) {
            mapStore.add((long) i, i);
        }

        byteBufferFactory = new FileBackedMemMappedByteBufferFactory(mapDir);
        store = mcsi.initializeMultiByteBufferBacked("boo", byteBufferFactory, 10, 512, true, 10, 10);
        factory = new ConcurrentFilerProviderBackedMapChunkFactory<>(8, false, 4, false, 16,
            new ConcurrentFilerProvider<>("booya".getBytes(), store));
        mapStore = new PartitionedMapChunkBackedMapStore<>(factory,
            new StripingLocksProvider<String>(8),
            null,
            zeroPartitioner,
            longIntegerMarshaller);

        for (int i = 0; i < 1_000; i++) {
            assertEquals(mapStore.get((long) i).intValue(), i);
        }
    }

    private void readAndAssertRandom(MultiChunkStoreConcurrentFilerFactory store, long seed) throws IOException {
        Random rand = new Random(seed);
        for (int i = 1; i < 1024; i = i * 2) {
            byte[] key = new byte[i];
            rand.nextBytes(key);
            try (ChunkFiler chunkFiler = store.get(key)) {
                chunkFiler.seek(0);
                assertEquals(FilerIO.readLong(chunkFiler, "noise"), rand.nextLong());
            }
        }
    }

    private void writeRandom(MultiChunkStoreConcurrentFilerFactory store, long seed) throws IOException {
        Random rand = new Random(seed);
        for (int i = 1; i < 1024; i = i * 2) {
            byte[] key = new byte[i];
            rand.nextBytes(key);
            try (ChunkFiler chunkFiler = store.allocate(key, 8)) {
                chunkFiler.seek(0);
                FilerIO.writeLong(chunkFiler, rand.nextLong(), "noise");
            }
        }
    }

    private final KeyPartitioner<Long> zeroPartitioner = new KeyPartitioner<Long>() {
        @Override
        public String keyPartition(Long key) {
            return "0";
        }

        @Override
        public Iterable<String> allPartitions() {
            return Collections.singletonList("0");
        }
    };

    private final KeyValueMarshaller<Long, Integer> longIntegerMarshaller = new KeyValueMarshaller<Long, Integer>() {

        @Override
        public byte[] keyBytes(Long key) {
            return FilerIO.longBytes(key);
        }

        @Override
        public byte[] valueBytes(Integer value) {
            return FilerIO.intBytes(value);
        }

        @Override
        public Long bytesKey(byte[] bytes, int offset) {
            return FilerIO.bytesLong(bytes, offset);
        }

        @Override
        public Integer bytesValue(Long key, byte[] bytes, int offset) {
            return FilerIO.bytesInt(bytes, offset);
        }
    };
}
