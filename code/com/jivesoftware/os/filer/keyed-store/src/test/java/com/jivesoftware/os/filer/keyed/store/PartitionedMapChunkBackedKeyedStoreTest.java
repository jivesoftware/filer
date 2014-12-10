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
import com.jivesoftware.os.filer.chunk.store.MultiChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import java.io.IOException;
import java.nio.file.Files;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

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
        String[] chunkDirs = new String[] {
            Files.createTempDirectory("chunk").toFile().getAbsolutePath(),
            Files.createTempDirectory("chunk").toFile().getAbsolutePath()
        };

        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        MultiChunkStore multiChunkStore = new MultiChunkStoreInitializer(chunkStoreInitializer)
            .initializeMultiFileBacked(chunkDirs, "data", 4, 4096, true, 8, 64);
        int newFilerInitialCapacity = 512;

        MapChunkFactory<ByteBufferBackedFiler> mapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, mapDirs);
        PartitionedMapChunkBackedKeyedStore<ByteBufferBackedFiler> partitionedMapChunkBackedKeyedStore = new PartitionedMapChunkBackedKeyedStore<>(
            mapChunkFactory, multiChunkStore, new StripingLocksProvider<String>(16), 4);

        byte[] key = FilerIO.intBytes(1010);
        partitionedMapChunkBackedKeyedStore.execute(key, newFilerInitialCapacity, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Filer filer) throws IOException {
                FilerIO.writeInt(filer, 10, "");
                return null;
            }
        });

        mapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, mapDirs);
        partitionedMapChunkBackedKeyedStore = new PartitionedMapChunkBackedKeyedStore<>(mapChunkFactory, multiChunkStore,
            new StripingLocksProvider<String>(16), 4);
        partitionedMapChunkBackedKeyedStore.execute(key, newFilerInitialCapacity, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Filer filer) throws IOException {
                filer.seek(0);
                int ten = FilerIO.readInt(filer, "");
                System.out.println("ten:" + ten);
                assertEquals(ten, 10);
                return null;
            }
        });
    }

    @Test
    public void rewriteTest() throws Exception {
        String[] mapDirs = new String[] {
            Files.createTempDirectory("map").toFile().getAbsolutePath(),
            Files.createTempDirectory("map").toFile().getAbsolutePath()
        };
        String[] chunkDirs = new String[] {
            Files.createTempDirectory("chunk").toFile().getAbsolutePath(),
            Files.createTempDirectory("chunk").toFile().getAbsolutePath()
        };

        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        MultiChunkStore multChunkStore = new MultiChunkStoreInitializer(chunkStoreInitializer)
            .initializeMultiFileBacked(chunkDirs, "data", 4, 4096, true, 8, 64);
        int newFilerInitialCapacity = 512;
        MapChunkFactory<ByteBufferBackedFiler> mapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, mapDirs);
        PartitionedMapChunkBackedKeyedStore<ByteBufferBackedFiler> partitionedMapChunkBackedKeyedStore = new PartitionedMapChunkBackedKeyedStore<>(
            mapChunkFactory, multChunkStore, new StripingLocksProvider<String>(16), 4);

        byte[] key = FilerIO.intBytes(1020);
        partitionedMapChunkBackedKeyedStore.execute(key, newFilerInitialCapacity, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Filer filer) throws IOException {
                filer.seek(0);
                FilerIO.writeInt(filer, 10, "");
                return null;
            }
        });
        partitionedMapChunkBackedKeyedStore.executeRewrite(key, newFilerInitialCapacity, new RewriteFilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Filer currentFiler, Filer newFiler) throws IOException {
                currentFiler.seek(0);
                int ten = FilerIO.readInt(currentFiler, "");
                assertEquals(ten, 10);

                newFiler.seek(0);
                FilerIO.writeInt(newFiler, 20, "");
                return null;
            }
        });

        mapChunkFactory = new FileBackedMapChunkFactory(4, false, 8, false, 100, mapDirs);
        partitionedMapChunkBackedKeyedStore = new PartitionedMapChunkBackedKeyedStore<>(mapChunkFactory, multChunkStore,
            new StripingLocksProvider<String>(16), 4);
        partitionedMapChunkBackedKeyedStore.execute(key, newFilerInitialCapacity, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Filer filer) throws IOException {
                filer.seek(0);
                int twenty = FilerIO.readInt(filer, "");
                assertEquals(twenty, 20);
                return null;
            }
        });
    }
}
