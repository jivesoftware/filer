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

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import java.util.Random;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class MultiChunkStoreTest {

    @Test
    public void testAllocate() throws Exception {

        //Files.createTempDirectory("map").toFile().getAbsolutePath()
        MultiChunkStoreInitializer mcsi = new MultiChunkStoreInitializer(new ChunkStoreInitializer());
        MultiChunkStoreConcurrentFilerFactory store = mcsi.initializeMultiByteBufferBacked("boo", new HeapByteBufferFactory(), 10, 512, true, 10, 10);

        Random rand = new Random(12345);
        for (int i = 1; i < 1024; i = i * 2) {
            byte[] key = new byte[i];
            rand.nextBytes(key);
            try (ChunkFiler chunkFiler = store.allocate(key, 8)) {
                chunkFiler.seek(0);
                FilerIO.writeLong(chunkFiler, rand.nextLong(), "noise");
            }
        }


//        ConcurrentFilerProviderBackedMapChunkFactory factory = new ConcurrentFilerProviderBackedMapChunkFactory(8, false, 8, false, 16,
//            new ConcurrentFilerProvider<>("booya".getBytes(),store));
//
//        PartitionedMapChunkBackedKeyedStore keyedStore = new PartitionedMapChunkBackedKeyedStore();


    }

}
