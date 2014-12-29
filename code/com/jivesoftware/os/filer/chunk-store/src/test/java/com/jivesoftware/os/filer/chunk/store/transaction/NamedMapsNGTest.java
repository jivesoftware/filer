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
import com.jivesoftware.os.filer.chunk.store.ChunkTransaction;
import com.jivesoftware.os.filer.chunk.store.RewriteChunkTransaction;
import com.jivesoftware.os.filer.io.ByteArrayPartitionFunction;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
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
public class NamedMapsNGTest {

    @Test
    public void testCommit() throws Exception {

        String chunkPath = Files.createTempDirectory("testNewChunkStore")
            .toFile()
            .getAbsolutePath();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().initialize(chunkPath, "data1", 10, true, 8);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().initialize(chunkPath, "data2", 10, true, 8);
        ChunkStore[] chunckStores = new ChunkStore[]{chunkStore1, chunkStore2};

        TxNamedMapOfFiler<Void> namedMapOfFilers = new TxNamedMapOfFiler<>(chunckStores, 464,
            new ByteArrayPartitionFunction(),
            new NoOpCreateFiler<ChunkFiler>(),
            new NoOpOpenFiler<ChunkFiler>(),
            new NoOpGrowFiler<Long, Void, ChunkFiler>());

        TxNamedMap namedMap = new TxNamedMap(chunckStores, 464, new ByteArrayPartitionFunction(),
            new MapCreator(2, 4, true, 8, false),
            new MapOpener(),
            new MapGrower<>(1));

        final int addCount = 16;
        for (int c = 0; c < 10; c++) {

            int accum = 0;
            for (int i = 0; i < addCount; i++) {
                accum = accum + i;
                final int key = i;

                namedMap.write(FilerIO.intBytes(c), "map1".getBytes(), new ChunkTransaction<MapContext, Void>() {

                    @Override
                    public Void commit(MapContext monkey, ChunkFiler filer) throws IOException {
                        MapStore.INSTANCE.add(filer, monkey, (byte) 1, String.valueOf(key)
                            .getBytes(), FilerIO.longBytes(key));
                        return null;
                    }
                });

                namedMapOfFilers.overwrite(FilerIO.intBytes(c), "filer1".getBytes(), (c + "overwrite").getBytes(), 8L,
                    new ChunkTransaction<Void, ChunkFiler>() {

                        @Override
                        public ChunkFiler commit(Void monkey, ChunkFiler filer) throws IOException {
                            filer.seek(0);
                            FilerIO.writeLong(filer, key, "value");
                            System.out.println("Overwrite:" + key + " " + filer.getChunkFP());
                            return null;
                        }
                    });

                namedMapOfFilers.rewrite(FilerIO.intBytes(c), "filer2".getBytes(), (c + "rewrite").getBytes(), 8L,
                    new RewriteChunkTransaction<Void, ChunkFiler>() {

                        @Override
                        public ChunkFiler commit(Void oldMonkey, ChunkFiler oldFiler, Void newMonkey, ChunkFiler newFiler) throws IOException {
                            long oldValue = 0;
                            if (oldFiler != null) {
                                oldFiler.seek(0);
                                oldValue = FilerIO.readLong(oldFiler, "value");
                                System.out.println("Old value:" + oldValue);
                            }
                            newFiler.seek(0);
                            FilerIO.writeLong(newFiler, oldValue + key, "value");
                            System.out.println("Rewrite:" + (oldValue + key) + " " + newFiler.getChunkFP());
                            return null;
                        }
                    });

                System.out.println("Accum:" + accum);
            }

            final AtomicBoolean failed = new AtomicBoolean();
            final int expectedAccum = accum;
            for (int i = 0; i < addCount; i++) {
                final int key = i;

                namedMap.read(FilerIO.intBytes(c), "map1".getBytes(), new ChunkTransaction<MapContext, Void>() {

                    @Override
                    public Void commit(MapContext monkey, ChunkFiler filer) throws IOException {
                        long i = MapStore.INSTANCE.get(filer, monkey, String.valueOf(key)
                            .getBytes());
                        long value = FilerIO.bytesLong(MapStore.INSTANCE.getPayload(filer, monkey, i));
                        //System.out.println("expected:" + key + " got:" + value + " from " + context.filer.getChunkFP());
                        if (value != key) {
                            System.out.println("mapRead FAILED. " + value + " vs " + key);
                            failed.set(true);
                        }
                        return null;
                    }
                });

                namedMapOfFilers.read(FilerIO.intBytes(c), "filer1".getBytes(), (c + "overwrite").getBytes(), new ChunkTransaction<Void, ChunkFiler>() {

                    @Override
                    public ChunkFiler commit(Void monkey, ChunkFiler filer) throws IOException {
                        filer.seek(0);
                        long v = FilerIO.readLong(filer, "value");
                        //System.out.println("OR:" + v);
                        if (v != addCount - 1) {
                            System.out.println("filerReadOverwrite FAILED. " + v + " vs " + (addCount - 1));
                            failed.set(true);
                        }
                        return null;
                    }
                });

                namedMapOfFilers.read(FilerIO.intBytes(c), "filer2".getBytes(), (c + "rewrite").getBytes(), new ChunkTransaction<Void, ChunkFiler>() {

                    @Override
                    public ChunkFiler commit(Void monkey, ChunkFiler filer) throws IOException {
                        filer.seek(0);
                        long v = FilerIO.readLong(filer, "value");
                        System.out.println("RR:" + v + " from " + filer.getChunkFP());
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

            int accum = 0;
            for (int i = 0; i < addCount; i++) {
                accum += i;
            }
            final int expectedAccum = accum;
            final AtomicBoolean failed = new AtomicBoolean();
            for (int i = 0; i < addCount; i++) {
                final int key = i;
                namedMap.read(FilerIO.intBytes(c), "map1".getBytes(), new ChunkTransaction<MapContext, Void>() {

                    @Override
                    public Void commit(MapContext monkey, ChunkFiler filer) throws IOException {
                        long i = MapStore.INSTANCE.get(filer, monkey, String.valueOf(key)
                            .getBytes());
                        long value = FilerIO.bytesLong(MapStore.INSTANCE.getPayload(filer, monkey, i));
                        //System.out.println("expected:" + key + " got:" + value + " from " + context.filer.getChunkFP());
                        if (value != key) {
                            System.out.println("on re-open mapRead FAILED. " + value + " vs " + key);
                            failed.set(true);
                        }
                        return null;
                    }
                });

                namedMapOfFilers.read(FilerIO.intBytes(c), "filer1".getBytes(), (c + "overwrite").getBytes(), new ChunkTransaction<Void, ChunkFiler>() {

                    @Override
                    public ChunkFiler commit(Void monkey, ChunkFiler filer) throws IOException {
                        filer.seek(0);
                        long v = FilerIO.readLong(filer, "value");
                        //System.out.println("OR:" + v);
                        if (v != addCount - 1) {
                            System.out.println("filerReadOverwrite FAILED. " + v + " vs " + (addCount - 1));
                            failed.set(true);
                        }
                        return null;
                    }
                });

                namedMapOfFilers.read(FilerIO.intBytes(c), "filer2".getBytes(), (c + "rewrite").getBytes(), new ChunkTransaction<Void, ChunkFiler>() {

                    @Override
                    public ChunkFiler commit(Void monkey, ChunkFiler filer) throws IOException {
                        filer.seek(0);
                        long v = FilerIO.readLong(filer, "value");
                        System.out.println("RR:" + v + " from " + filer.getChunkFP());
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

        namedMapOfFilers.stream("filer2".getBytes(), new TxStream<byte[], Void, ChunkFiler>() {

            @Override
            public boolean stream(byte[] key, Void monkey, ChunkFiler filer) throws IOException {
                System.out.println(new IBA(key) + " " + filer);
                return true;
            }
        });

        namedMap.stream("map1".getBytes(), new TxStream<byte[], MapContext, ChunkFiler>() {

            @Override
            public boolean stream(byte[] key, MapContext monkey, ChunkFiler filer) throws IOException {
                MapStore.INSTANCE.streamKeys(filer, monkey, new MapStore.KeyStream() {

                    @Override
                    public boolean stream(byte[] key) throws IOException {
                        System.out.println(new IBA(key));
                        return true;
                    }
                });
                return true;
            }
        });
    }

}
