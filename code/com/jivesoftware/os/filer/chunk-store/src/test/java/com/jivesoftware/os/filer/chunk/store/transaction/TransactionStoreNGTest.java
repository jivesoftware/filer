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

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteArrayPartitionFunction;
import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.LocksProvider;
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
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
        ChunkStore chunkStore1 = new ChunkStoreInitializer().initialize(chunkPath, "data1", 10, true, 8);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().initialize(chunkPath, "data2", 10, true, 8);
        ChunkStore[] chunckStores = new ChunkStore[]{chunkStore1, chunkStore2};

        LocksProvider<byte[]> locksProvider = new ByteArrayStripingLocksProvider(10);
        LocksProvider<Integer> intLocksProvider = new StripingLocksProvider<>(10);
        final int addCount = 16;
        for (int c = 0; c < 10; c++) {

            TransactionBuilder rootWrite = new TransactionBuilder<KeyedFPIndex<Long>>()
                .add(FilerIO.intBytes(c), new ChunkPartitionerLevel(ByteArrayPartitionFunction.INSTANCE))
                .add(_skyHookFP, new ConstantFPLevel<Long>())
                .add("skyHook".getBytes(), new KeyedFPIndexWriteLevel())
                .add("table".length(), new KeyedMapFPIndexWriteLevel(intLocksProvider, true))
                .add("table".getBytes(), new KeyedFPIndexWriteLevel());

            TransactionBuilder rootRead = new TransactionBuilder<KeyedFPIndex<Long>>()
                .add(FilerIO.intBytes(c), new ChunkPartitionerLevel(ByteArrayPartitionFunction.INSTANCE))
                .add(_skyHookFP, new ConstantFPLevel<Long>())
                .add("skyHook".getBytes(), new KeyedFPIndexReadLevel())
                .add("table".length(), new KeyedMapFPIndexReadLevel(intLocksProvider))
                .add("table".getBytes(), new KeyedFPIndexReadLevel());

            TransactionBuilder mapWrite = rootWrite
                .add("row".length(), new KeyedMapFPIndexWriteLevel(intLocksProvider, true))
                .add("row".getBytes(), new KeyedMapWriteLevel(new MapCreator(2, 3, true, 8, false), MapOpener.DEFAULT, locksProvider, 1));

            TransactionBuilder mapRead = rootRead
                .add("row".length(), new KeyedMapFPIndexReadLevel(intLocksProvider))
                .add("row".getBytes(), new KeyedMapReadLevel(MapOpener.DEFAULT, locksProvider));

            TransactionBuilder filerOverwrite = rootWrite
                .add((c + "overwrite").length(), new KeyedMapFPIndexWriteLevel(intLocksProvider, true))
                .add((c + "overwrite").getBytes(),
                    new KeyedFilerOverwriteLevel(locksProvider, 8, new NoOpCreateFiler<ChunkFiler>(), new NoOpOpenFiler<ChunkFiler>()));

            TransactionBuilder filerReadOverwrite = rootRead
                .add((c + "overwrite").length(), new KeyedMapFPIndexReadLevel(intLocksProvider))
                .add((c + "overwrite").getBytes(), new KeyedFilerReadOnlyLevel(locksProvider, new NoOpOpenFiler<ChunkFiler>()));

            TransactionBuilder filerRewrite = rootWrite
                .add((c + "rewrite").length(), new KeyedMapFPIndexWriteLevel(intLocksProvider, true))
                .add((c + "rewrite").getBytes(), new KeyedFilerRewriteLevel(locksProvider, 8,
                        new NoOpCreateFiler<ChunkFiler>(), new NoOpOpenFiler<ChunkFiler>()));

            TransactionBuilder filerReadRewrite = rootRead
                .add((c + "rewrite").length(), new KeyedMapFPIndexReadLevel(intLocksProvider))
                .add((c + "rewrite").getBytes(), new KeyedFilerReadOnlyLevel(locksProvider, new NoOpOpenFiler<ChunkFiler>()));

            int accum = 0;
            for (int i = 0; i < addCount; i++) {
                accum = accum + i;
                final int key = i;
                mapWrite.commit(chunckStores, new StoreTransaction<Void, ChunkStore, MonkeyAndFiler<MapContext>>() {

                    @Override
                    public Void commit(ChunkStore store, MonkeyAndFiler<MapContext> context) throws IOException {
                        MapStore.INSTANCE.add(context.filer, context.monkey, (byte) 1, String.valueOf(key).getBytes(), FilerIO.longBytes(key));
                        return null;
                    }
                });

                filerOverwrite.commit(chunckStores, new StoreTransaction<Void, ChunkStore, MonkeyAndFiler<Void>>() {
                    @Override
                    public Void commit(ChunkStore backingStorage, MonkeyAndFiler<Void> context) throws IOException {
                        context.filer.seek(0);
                        FilerIO.writeLong(context.filer, key, "value");
                        System.out.println("Overwrite:" + key + " " + context.filer.getChunkFP());
                        return null;
                    }
                });

                filerRewrite.commit(chunckStores, new StoreTransaction<Void, ChunkStore, KeyedFilerRewriteLevel.RewriteFiler<Void>>() {
                    @Override
                    public Void commit(ChunkStore backingStorage, KeyedFilerRewriteLevel.RewriteFiler<Void> context) throws IOException {
                        long oldValue = 0;
                        if (context.oldFiler != null) {
                            context.oldFiler.seek(0);
                            oldValue = FilerIO.readLong(context.oldFiler, "value");
                            System.out.println("Old value:" + oldValue);
                        }
                        context.newFiler.seek(0);
                        FilerIO.writeLong(context.newFiler, oldValue + key, "value");
                        System.out.println("Rewrite:" + (oldValue + key) + " " + context.newFiler.getChunkFP());
                        return null;
                    }
                });
                System.out.println("Accum:" + accum);

            }

            final AtomicBoolean failed = new AtomicBoolean();
            final int expectedAccum = accum;
            for (int i = 0; i < addCount; i++) {
                final int key = i;
                mapRead.commit(chunckStores, new StoreTransaction<Void, ChunkStore, MonkeyAndFiler<MapContext>>() {

                    @Override
                    public Void commit(ChunkStore chunkStore, MonkeyAndFiler<MapContext> context) throws IOException {
                        long i = MapStore.INSTANCE.get(context.filer, context.monkey, String.valueOf(key).getBytes());
                        long value = FilerIO.bytesLong(MapStore.INSTANCE.getPayload(context.filer, context.monkey, i));
                        //System.out.println("expected:" + key + " got:" + value + " from " + context.filer.getChunkFP());
                        if (value != key) {
                            System.out.println("mapRead FAILED. " + value + " vs " + key);
                            failed.set(true);
                        }
                        return null;
                    }
                });

                filerReadOverwrite.commit(chunckStores, new StoreTransaction<Void, ChunkStore, MonkeyAndFiler<Void>>() {
                    @Override
                    public Void commit(ChunkStore backingStorage, MonkeyAndFiler<Void> context) throws IOException {
                        context.filer.seek(0);
                        long v = FilerIO.readLong(context.filer, "value");
                        //System.out.println("OR:" + v);
                        if (v != addCount - 1) {
                            System.out.println("filerReadOverwrite FAILED. " + v + " vs " + (addCount - 1));
                            failed.set(true);
                        }
                        return null;
                    }
                });

                filerReadRewrite.commit(chunckStores, new StoreTransaction<Void, ChunkStore, MonkeyAndFiler<Void>>() {
                    @Override
                    public Void commit(ChunkStore backingStorage, MonkeyAndFiler<Void> context) throws IOException {
                        context.filer.seek(0);
                        long v = FilerIO.readLong(context.filer, "value");
                        System.out.println("RR:" + v + " from " + context.filer.getChunkFP());
                        if (v != expectedAccum) {
                            System.out.println("filerReadRewrite FAILED. " + v + " vs " + expectedAccum);
                            failed.set(true);
                        }
                        return null;
                    }
                });
            }
            Assert.assertFalse(failed.get());

        }

        chunkStore1 = new ChunkStoreInitializer().initialize(chunkPath, "data1", 10, true, 8);
        chunkStore2 = new ChunkStoreInitializer().initialize(chunkPath, "data2", 10, true, 8);
        chunckStores = new ChunkStore[]{chunkStore1, chunkStore2};

        for (int c = 0; c < 10; c++) {

            TransactionBuilder rootRead = new TransactionBuilder<KeyedFPIndex<Long>>()
                .add(FilerIO.intBytes(c), new ChunkPartitionerLevel(ByteArrayPartitionFunction.INSTANCE))
                .add(_skyHookFP, new ConstantFPLevel<Long>())
                .add("skyHook".getBytes(), new KeyedFPIndexReadLevel())
                .add("table".length(), new KeyedMapFPIndexReadLevel(intLocksProvider))
                .add("table".getBytes(), new KeyedFPIndexReadLevel());

            TransactionBuilder mapRead = rootRead
                .add("row".length(), new KeyedMapFPIndexReadLevel(intLocksProvider))
                .add("row".getBytes(), new KeyedMapReadLevel(MapOpener.DEFAULT, locksProvider));

            TransactionBuilder filerReadOverwrite = rootRead
                .add((c + "overwrite").length(), new KeyedMapFPIndexReadLevel(intLocksProvider))
                .add((c + "overwrite").getBytes(), new KeyedFilerReadOnlyLevel(locksProvider, new NoOpOpenFiler<ChunkFiler>()));

            TransactionBuilder filerReadRewrite = rootRead
                .add((c + "rewrite").length(), new KeyedMapFPIndexReadLevel(intLocksProvider))
                .add((c + "rewrite").getBytes(), new KeyedFilerReadOnlyLevel(locksProvider, new NoOpOpenFiler<ChunkFiler>()));

            int accum = 0;
            for (int i = 0; i < addCount; i++) {
                accum += i;
            }
            final int expectedAccum = accum;
            final AtomicBoolean failed = new AtomicBoolean();
            for (int i = 0; i < addCount; i++) {
                final int key = i;
                mapRead.commit(chunckStores, new StoreTransaction<Void, ChunkStore, MonkeyAndFiler<MapContext>>() {

                    @Override
                    public Void commit(ChunkStore chunkStore, MonkeyAndFiler<MapContext> context) throws IOException {
                        long i = MapStore.INSTANCE.get(context.filer, context.monkey, String.valueOf(key).getBytes());
                        long value = FilerIO.bytesLong(MapStore.INSTANCE.getPayload(context.filer, context.monkey, i));
                        //System.out.println("expected:" + key + " got:" + value + " from " + context.filer.getChunkFP());
                        if (value != key) {
                            System.out.println("on re-open mapRead FAILED. " + value + " vs " + key);
                            failed.set(true);
                        }
                        return null;
                    }
                });

                filerReadOverwrite.commit(chunckStores, new StoreTransaction<Void, ChunkStore, MonkeyAndFiler<Void>>() {
                    @Override
                    public Void commit(ChunkStore backingStorage, MonkeyAndFiler<Void> context) throws IOException {
                        context.filer.seek(0);
                        long v = FilerIO.readLong(context.filer, "value");
                        System.out.println("OR:" + v);
                        if (v != addCount - 1) {
                            System.out.println("on re-open filerReadOverwrite FAILED. " + v + " vs " + (addCount - 1));
                            failed.set(true);
                        }
                        return null;
                    }
                });

                filerReadRewrite.commit(chunckStores, new StoreTransaction<Void, ChunkStore, MonkeyAndFiler<Void>>() {
                    @Override
                    public Void commit(ChunkStore backingStorage, MonkeyAndFiler<Void> context) throws IOException {
                        context.filer.seek(0);
                        long v = FilerIO.readLong(context.filer, "value");
                        System.out.println("RR:" + v);
                        if (v != expectedAccum) {
                            System.out.println("on re-open filerReadRewrite FAILED. " + v + " vs " + expectedAccum);
                            failed.set(true);
                        }
                        return null;
                    }
                });
            }
            Assert.assertFalse(failed.get());

        }
    }

}
