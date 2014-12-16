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
package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class TransactionStoreNGTest {

    final long _skyHookFP = 464; // I died a little bit doing this.

    @Test
    public void testCommit() throws Exception {
        String chunkPath = Files.createTempDirectory("testNewChunkStore").toFile().getAbsolutePath();
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunkPath, "data", 10, true, 8);
        TransactionStore transactionStore = new TransactionStore();

        List<LevelProvider> levelProviders = new ArrayList<>();
        levelProviders.add(new PowerToFPLevel(chunkStore));
        levelProviders.add(new KeySizeToFPLevel(chunkStore, true));

        transactionStore.commit(Arrays.asList(_skyHookFP, 8),
            levelProviders, new StoreTransaction<Void, MapContextFiler>() {

            @Override
            public Void commit(MapContextFiler store) throws IOException {
                MapStore.INSTANCE.add(store.filer, store.mapContext, (byte) 1, "booya".getBytes(), FilerIO.longBytes(1234));
                return null;
            }
            });

        transactionStore.commit(Arrays.asList(_skyHookFP, 8),
            levelProviders, new StoreTransaction<Void, MapContextFiler>() {

                @Override
                public Void commit(MapContextFiler store) throws IOException {
                    long i = MapStore.INSTANCE.get(store.filer, store.mapContext, "booya".getBytes());
                    long value = FilerIO.bytesLong(MapStore.INSTANCE.getPayload(store.filer, store.mapContext, i));
                    Assert.assertEquals(value, 1234L);
                    return null;
                }
            });

        levelProviders.clear();
        chunkStore = new ChunkStoreInitializer().initialize(chunkPath, "data", 1024 * 1024 * 10, true, 8);
        transactionStore = new TransactionStore();
        levelProviders.add(new PowerToFPLevel(chunkStore));
        levelProviders.add(new KeySizeToFPLevel(chunkStore, true));

        transactionStore.commit(Arrays.asList(_skyHookFP, 8),
            levelProviders, new StoreTransaction<Void, MapContextFiler>() {

                @Override
                public Void commit(MapContextFiler store) throws IOException {
                    long i = MapStore.INSTANCE.get(store.filer, store.mapContext, "booya".getBytes());
                    long value = FilerIO.bytesLong(MapStore.INSTANCE.getPayload(store.filer, store.mapContext, i));
                    Assert.assertEquals(value, 1234L);
                    return null;
                }
            });
    }

    @Test
    public void testFilers() throws Exception {
        String chunkPath = Files.createTempDirectory("testNewChunkStore").toFile().getAbsolutePath();
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunkPath, "data", 10, true, 8);
        ByteArrayStripingLocksProvider locksProvider = new ByteArrayStripingLocksProvider(32);
        TransactionStore transactionStore = new TransactionStore();

        List<LevelProvider> levelProviders = new ArrayList<>();
        levelProviders.add(new PowerToFPLevel(chunkStore));
        levelProviders.add(new KeySizeToFPLevel(chunkStore, true));
        levelProviders.add(new KeyedPayloadsWriteLevel(chunkStore, 8, true, 8, false, 1));
        levelProviders.add(new RewriteFilerLevel(chunkStore, locksProvider, 8));

        for (int i = 0; i < 10; i++) {
            transactionStore.commit(Arrays.asList(_skyHookFP, 8, "foo".getBytes(), "bar".getBytes()),
                levelProviders, new StoreTransaction<Void, RewriteFilerLevel.RewriteFiler>() {

                @Override
                public Void commit(RewriteFilerLevel.RewriteFiler store) throws IOException {
                    // addition for the win.
                    long count = 0;
                    if (store.oldFiler != null) {
                        count = FilerIO.readLong(store.oldFiler, "count");
                    }
                    count++;
                    FilerIO.writeLong(store.newFiler, count, "count");
                    return null;
                }

                });
        }

        levelProviders.clear();
        levelProviders.add(new PowerToFPLevel(chunkStore));
        levelProviders.add(new KeySizeToFPLevel(chunkStore, true));
        levelProviders.add(new KeyedPayloadsWriteLevel(chunkStore, 8, true, 8, false, 1));
        levelProviders.add(new ReadOnlyFilerLevel(chunkStore, locksProvider));
        transactionStore.commit(Arrays.asList(_skyHookFP, 8, "foo".getBytes(), "bar".getBytes()),
            levelProviders, new StoreTransaction<Void, Filer>() {

                @Override
                public Void commit(Filer filer) throws IOException {
                    // addition for the win.
                    long count = FilerIO.readLong(filer, "count");
                    Assert.assertEquals(count, 10);
                    return null;

                }
            });
    }

}
