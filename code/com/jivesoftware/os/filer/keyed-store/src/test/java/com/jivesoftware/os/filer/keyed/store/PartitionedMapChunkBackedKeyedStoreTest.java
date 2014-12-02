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
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import java.nio.file.Files;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class PartitionedMapChunkBackedKeyedStoreTest {

    @Test
    public void keyedStoreTest() throws Exception {
        String[] mapDirs = new String[] {
            Files.createTempDirectory("map").toFile().getAbsolutePath(),
            Files.createTempDirectory("map").toFile().getAbsolutePath()
        };
        String[] swapDirs = new String[] {
            Files.createTempDirectory("swap").toFile().getAbsolutePath(),
            Files.createTempDirectory("swap").toFile().getAbsolutePath()
        };
        String[] chunkDirs = new String[] {
            Files.createTempDirectory("chunk").toFile().getAbsolutePath(),
            Files.createTempDirectory("chunk").toFile().getAbsolutePath()
        };

        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        MultiChunkStore multChunkStore = chunkStoreInitializer.initializeMultiFileBacked(chunkDirs, "data", 4, 4096, false, 8,
            new ByteArrayStripingLocksProvider(64));
        int newFilerInitialCapacity = 512;

        MapChunkFactory mapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, mapDirs);
        MapChunkFactory swapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, swapDirs);
        PartitionedMapChunkBackedKeyedStore partitionedMapChunkBackedKeyedStore = new PartitionedMapChunkBackedKeyedStore(
            mapChunkFactory, swapChunkFactory, multChunkStore, new StripingLocksProvider<String>(16), 4);

        byte[] key = FilerIO.intBytes(1010);
        Filer filer = partitionedMapChunkBackedKeyedStore.get(key, newFilerInitialCapacity);
        synchronized (filer.lock()) {
            FilerIO.writeInt(filer, 10, "");
        }

        mapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, mapDirs);
        swapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, swapDirs);
        partitionedMapChunkBackedKeyedStore = new PartitionedMapChunkBackedKeyedStore(mapChunkFactory, swapChunkFactory, multChunkStore,
            new StripingLocksProvider<String>(16), 4);
        filer = partitionedMapChunkBackedKeyedStore.get(key, newFilerInitialCapacity);
        synchronized (filer.lock()) {
            filer.seek(0);
            int ten = FilerIO.readInt(filer, "");
            System.out.println("ten:" + ten);
            Assert.assertEquals(ten, 10);
        }
    }

    @Test
    public void swapTest() throws Exception {
        String[] mapDirs = new String[] {
            Files.createTempDirectory("map").toFile().getAbsolutePath(),
            Files.createTempDirectory("map").toFile().getAbsolutePath()
        };
        String[] swapDirs = new String[] {
            Files.createTempDirectory("swap").toFile().getAbsolutePath(),
            Files.createTempDirectory("swap").toFile().getAbsolutePath()
        };
        String[] chunkDirs = new String[] {
            Files.createTempDirectory("chunk").toFile().getAbsolutePath(),
            Files.createTempDirectory("chunk").toFile().getAbsolutePath()
        };

        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        MultiChunkStore multChunkStore = chunkStoreInitializer.initializeMultiFileBacked(chunkDirs, "data", 4, 4096, false, 8,
            new ByteArrayStripingLocksProvider(64));
        int newFilerInitialCapacity = 512;
        MapChunkFactory mapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, mapDirs);
        MapChunkFactory swapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, swapDirs);
        PartitionedMapChunkBackedKeyedStore partitionedMapChunkBackedKeyedStore = new PartitionedMapChunkBackedKeyedStore(
            mapChunkFactory, swapChunkFactory, multChunkStore, new StripingLocksProvider<String>(16), 4);

        byte[] key = FilerIO.intBytes(1020);
        SwappableFiler filer = partitionedMapChunkBackedKeyedStore.get(key, newFilerInitialCapacity);
        synchronized (filer.lock()) {
            filer.sync();
            FilerIO.writeInt(filer, 10, "");

            SwappingFiler swappingFiler = filer.swap(4);
            FilerIO.writeInt(swappingFiler, 20, "");
            swappingFiler.commit();
        }

        mapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, mapDirs);
        swapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, swapDirs);
        partitionedMapChunkBackedKeyedStore = new PartitionedMapChunkBackedKeyedStore(mapChunkFactory, swapChunkFactory, multChunkStore,
            new StripingLocksProvider<String>(16), 4);
        filer = partitionedMapChunkBackedKeyedStore.get(key, newFilerInitialCapacity);
        synchronized (filer.lock()) {
            filer.sync();
            filer.seek(0);
            int twenty = FilerIO.readInt(filer, "");
            System.out.println("twenty:" + twenty);
            Assert.assertEquals(twenty, 20);
        }
    }
}
