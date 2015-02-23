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
import com.jivesoftware.os.filer.chunk.store.transaction.TxNamedMapOfFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author jonathan.colt
 */
public class TxKeyedFilerStoreNGTest {

    @Test
    public void keyedStoreTest() throws Exception {
        assertKeyedStoreTest(false);
    }

    @Test
    public void orderedKeyedStoreTest() throws Exception {
        assertKeyedStoreTest(true);
    }

    private void assertKeyedStoreTest(boolean lexOrderKeys) throws Exception, IOException {
        File dir = Files.createTempDirectory("testNewChunkStore").toFile();
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data1", 8, byteBufferFactory, 5_000);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data2", 8, byteBufferFactory, 5_000);
        ChunkStore[] chunkStores = new ChunkStore[]{chunkStore1, chunkStore2};

        TxKeyedFilerStore store = new TxKeyedFilerStore(chunkStores,
            "booya".getBytes(),
            lexOrderKeys,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        long newFilerInitialCapacity = 512;

        byte[] key = FilerIO.intBytes(1010);
        store.readWriteAutoGrow(key, newFilerInitialCapacity, new ChunkTransaction<Void, Void>() {
            @Override
            public Void commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    FilerIO.writeInt(filer, 10, "");
                    return null;
                }
            }
        });

        store.readWriteAutoGrow(key, newFilerInitialCapacity, new ChunkTransaction<Void, Void>() {
            @Override
            public Void commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    filer.seek(0);
                    int ten = FilerIO.readInt(filer, "");
                    System.out.println("ten:" + ten);
                    assertEquals(ten, 10);
                    return null;
                }
            }
        });

        List<KeyRange> ranges = new ArrayList<>();
        ranges.add(new KeyRange(FilerIO.intBytes(900), FilerIO.intBytes(1010)));
        final AtomicInteger count = new AtomicInteger();
        store.streamKeys(ranges, new KeyValueStore.KeyStream<IBA>() {

            @Override
            public boolean stream(IBA key) throws IOException {
                count.incrementAndGet();
                return true;
            }
        });

        Assert.assertEquals(count.get(), 0);

        ranges.add(new KeyRange(FilerIO.intBytes(1011), FilerIO.intBytes(1030)));
        store.streamKeys(ranges, new KeyValueStore.KeyStream<IBA>() {

            @Override
            public boolean stream(IBA key) throws IOException {
                count.incrementAndGet();
                return true;
            }
        });

        Assert.assertEquals(count.get(), 0);

        ranges.add(new KeyRange(FilerIO.intBytes(1010), FilerIO.intBytes(1030)));
        store.streamKeys(ranges, new KeyValueStore.KeyStream<IBA>() {

            @Override
            public boolean stream(IBA key) throws IOException {
                count.incrementAndGet();
                return true;
            }
        });

        Assert.assertEquals(count.get(), 1);
    }

    @Test
    public void rewriteTest() throws Exception {
        assertRewriteTest(false);
    }

    @Test
    public void orderedRewriteTest() throws Exception {
        assertRewriteTest(true);
    }

    private void assertRewriteTest(boolean lexOrderKeys) throws IOException, Exception {
        File dir = Files.createTempDirectory("testNewChunkStore").toFile();
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data1", 8, byteBufferFactory, 5_000);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data2", 8, byteBufferFactory, 5_000);
        ChunkStore[] chunkStores = new ChunkStore[]{chunkStore1, chunkStore2};

        TxKeyedFilerStore store = new TxKeyedFilerStore(chunkStores,
            "booya".getBytes(),
            lexOrderKeys,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        long newFilerInitialCapacity = 512;

        final byte[] key = FilerIO.intBytes(1020);
        store.readWriteAutoGrow(key, newFilerInitialCapacity, new ChunkTransaction<Void, Void>() {
            @Override
            public Void commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    filer.seek(0);
                    FilerIO.writeInt(filer, 10, "");
                    return null;
                }
            }
        });
        store.writeNewReplace(key, newFilerInitialCapacity, new ChunkTransaction<Void, Void>() {
            @Override
            public Void commit(Void monkey, ChunkFiler newFiler, Object newLock) throws IOException {
                synchronized (newLock) {
                    newFiler.seek(0);
                    FilerIO.writeInt(newFiler, 20, "");
                    return null;
                }
            }
        });

        store = new TxKeyedFilerStore(chunkStores,
            "booya".getBytes(),
            lexOrderKeys,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        store.readWriteAutoGrow(key, newFilerInitialCapacity, new ChunkTransaction<Void, Void>() {
            @Override
            public Void commit(Void monkey, ChunkFiler filer, Object lock) throws IOException {
                synchronized (lock) {
                    filer.seek(0);
                    int twenty = FilerIO.readInt(filer, "");
                    assertEquals(twenty, 20);
                    return null;
                }
            }
        });

        store.stream(null, new KeyValueStore.EntryStream<IBA, Filer>() {

            @Override
            public boolean stream(IBA k, Filer filer) throws IOException {
                assertEquals(k.getBytes(), key);
                filer.seek(0);
                int twenty = FilerIO.readInt(filer, "");
                assertEquals(twenty, 20);
                return true;
            }
        });

        store.streamKeys(null, new KeyValueStore.KeyStream<IBA>() {

            @Override
            public boolean stream(IBA k) throws IOException {
                assertEquals(k.getBytes(), key);
                return true;
            }
        });
    }
}
