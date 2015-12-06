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
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author jonathan.colt
 */
public class TxKeyedFilerStoreNGTest {

    TxCogs cogs = new TxCogs(256, 64, null, null, null);

    @Test
    public void keyedStoreTest() throws Exception {
        assertKeyedStoreTest(false);
    }

    @Test
    public void orderedKeyedStoreTest() throws Exception {
        assertKeyedStoreTest(true);
    }

    private void assertKeyedStoreTest(boolean lexOrderKeys) throws Exception, IOException {
        StackBuffer stackBuffer = new StackBuffer();
        File dir = Files.createTempDirectory("testNewChunkStore").toFile();
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data1", 8, byteBufferFactory, 500, 5_000, stackBuffer);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data2", 8, byteBufferFactory, 500, 5_000, stackBuffer);
        ChunkStore[] chunkStores = new ChunkStore[]{chunkStore1, chunkStore2};

        TxKeyedFilerStore<Long, Void> store = new TxKeyedFilerStore<>(cogs,
            0,
            chunkStores,
            "booya".getBytes(),
            lexOrderKeys,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        long newFilerInitialCapacity = 512;

        byte[] key = FilerIO.intBytes(1010);
        store.readWriteAutoGrow(key, newFilerInitialCapacity, (monkey, filer, _stackBuffer, lock) -> {
            synchronized (lock) {
                FilerIO.writeInt(filer, 10, "", _stackBuffer);
                return null;
            }
        }, stackBuffer);

        store.readWriteAutoGrow(key, newFilerInitialCapacity, (monkey, filer, _stackBuffer, lock) -> {
            synchronized (lock) {
                filer.seek(0);
                int ten = FilerIO.readInt(filer, "", _stackBuffer);
                System.out.println("ten:" + ten);
                assertEquals(ten, 10);
                return null;
            }
        }, stackBuffer);

        List<KeyRange> ranges = new ArrayList<>();
        ranges.add(new KeyRange(FilerIO.intBytes(900), FilerIO.intBytes(1010)));
        final AtomicInteger count = new AtomicInteger();
        store.streamKeys(ranges, key1 -> {
            count.incrementAndGet();
            return true;
        }, stackBuffer);

        Assert.assertEquals(count.get(), 0);

        ranges.add(new KeyRange(FilerIO.intBytes(1011), FilerIO.intBytes(1030)));
        store.streamKeys(ranges, key1 -> {
            count.incrementAndGet();
            return true;
        }, stackBuffer);

        Assert.assertEquals(count.get(), 0);

        ranges.add(new KeyRange(FilerIO.intBytes(1010), FilerIO.intBytes(1030)));
        store.streamKeys(ranges, key1 -> {
            count.incrementAndGet();
            return true;
        }, stackBuffer);

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
        StackBuffer stackBuffer = new StackBuffer();

        File dir = Files.createTempDirectory("testNewChunkStore").toFile();
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data1", 8, byteBufferFactory, 500, 5_000, stackBuffer);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data2", 8, byteBufferFactory, 500, 5_000, stackBuffer);
        ChunkStore[] chunkStores = new ChunkStore[]{chunkStore1, chunkStore2};

        TxKeyedFilerStore<Long, Void> store = new TxKeyedFilerStore<>(cogs,
            0,
            chunkStores,
            "booya".getBytes(),
            lexOrderKeys,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        long newFilerInitialCapacity = 512;

        final byte[] key = FilerIO.intBytes(1020);
        store.readWriteAutoGrow(key, newFilerInitialCapacity, (monkey, filer, _stackBuffer, lock) -> {
            synchronized (lock) {
                filer.seek(0);
                FilerIO.writeInt(filer, 10, "", _stackBuffer);
                return null;
            }
        }, stackBuffer);
        store.writeNewReplace(key, newFilerInitialCapacity, (monkey, newFiler, _stackBuffer, newLock) -> {
            synchronized (newLock) {
                newFiler.seek(0);
                FilerIO.writeInt(newFiler, 20, "", _stackBuffer);
                return null;
            }
        }, stackBuffer);

        store = new TxKeyedFilerStore<>(cogs,
            0,
            chunkStores,
            "booya".getBytes(),
            lexOrderKeys,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        store.readWriteAutoGrow(key, newFilerInitialCapacity, (monkey, filer, _stackBuffer, lock) -> {
            synchronized (lock) {
                filer.seek(0);
                int twenty = FilerIO.readInt(filer, "", _stackBuffer);
                assertEquals(twenty, 20);
                return null;
            }
        }, stackBuffer);

        store.stream(null, (k, filer) -> {
            assertEquals(k, key);
            filer.seek(0);
            int twenty = FilerIO.readInt(filer, "", stackBuffer);
            assertEquals(twenty, 20);
            return true;
        }, stackBuffer);

        store.streamKeys(null, k -> {
            assertEquals(k, key);
            return true;
        }, stackBuffer);
    }

    @Test
    public void readEachTest() throws Exception {
        assertReadEachTest(false);
    }

    @Test
    public void orderedReadEachTest() throws Exception {
        assertReadEachTest(true);
    }

    private void assertReadEachTest(boolean lexOrderKeys) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        File dir = Files.createTempDirectory("testNewChunkStore").toFile();
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data1", 8, byteBufferFactory, 500, 5_000, stackBuffer);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().openOrCreate(new File[]{dir}, 0, "data2", 8, byteBufferFactory, 500, 5_000, stackBuffer);
        ChunkStore[] chunkStores = new ChunkStore[]{chunkStore1, chunkStore2};

        TxKeyedFilerStore<Long, Void> store = new TxKeyedFilerStore<>(cogs,
            0,
            chunkStores,
            "booya".getBytes(),
            lexOrderKeys,
            TxNamedMapOfFiler.CHUNK_FILER_CREATOR,
            TxNamedMapOfFiler.CHUNK_FILER_OPENER,
            TxNamedMapOfFiler.OVERWRITE_GROWER_PROVIDER,
            TxNamedMapOfFiler.REWRITE_GROWER_PROVIDER);

        long newFilerInitialCapacity = 512;

        for (int i = 0; i < 20; i++) {
            final int value = i;
            final byte[] key = FilerIO.intBytes(i);
            store.readWriteAutoGrow(key, newFilerInitialCapacity, (monkey, filer, _stackBuffer, lock) -> {
                synchronized (lock) {
                    filer.seek(0);
                    FilerIO.writeInt(filer, value, "", _stackBuffer);
                    return null;
                }
            }, stackBuffer);
        }

        byte[][] allKeys = new byte[20][];
        for (int i = 0; i < 20; i++) {
            allKeys[i] = FilerIO.intBytes(i);
        }

        Integer[] results = new Integer[20];
        store.readEach(allKeys, newFilerInitialCapacity, (monkey, filer, _stackBuffer, lock, index) -> {
            synchronized (lock) {
                filer.seek(0);
                return FilerIO.readInt(filer, "", _stackBuffer);
            }
        }, results, stackBuffer);

        //Collections.sort(values);
        for (int i = 0; i < 20; i++) {
            assertEquals(results[i].intValue(), i);
        }

        assertEquals(store.size(stackBuffer), 20);
    }
}
