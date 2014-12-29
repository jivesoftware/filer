/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.io.RewriteFilerTransaction;
import java.io.IOException;
import java.nio.file.Files;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author jonathan.colt
 */
public class TxKeyedFilerStoreTest {

    @Test
    public void keyedStoreTest() throws Exception {
        String chunkPath = Files.createTempDirectory("testNewChunkStore")
            .toFile()
            .getAbsolutePath();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().initialize(chunkPath, "data1", 10, true, 8);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().initialize(chunkPath, "data2", 10, true, 8);
        ChunkStore[] chunkStores = new ChunkStore[]{chunkStore1, chunkStore2};

        TxKeyedFilerStore store = new TxKeyedFilerStore(chunkStores,
            "booya".getBytes());

        int newFilerInitialCapacity = 512;

        byte[] key = FilerIO.intBytes(1010);
        store.execute(key, newFilerInitialCapacity, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Filer filer) throws IOException {
                FilerIO.writeInt(filer, 10, "");
                return null;
            }
        });

        store.execute(key, newFilerInitialCapacity, new FilerTransaction<Filer, Void>() {
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
        String chunkPath = Files.createTempDirectory("testNewChunkStore")
            .toFile()
            .getAbsolutePath();
        ChunkStore chunkStore1 = new ChunkStoreInitializer().initialize(chunkPath, "data1", 10, true, 8);
        ChunkStore chunkStore2 = new ChunkStoreInitializer().initialize(chunkPath, "data2", 10, true, 8);
        ChunkStore[] chunkStores = new ChunkStore[]{chunkStore1, chunkStore2};

        TxKeyedFilerStore store = new TxKeyedFilerStore(chunkStores,
            "booya".getBytes());

        int newFilerInitialCapacity = 512;

        byte[] key = FilerIO.intBytes(1020);
        store.execute(key, newFilerInitialCapacity, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Filer filer) throws IOException {
                filer.seek(0);
                FilerIO.writeInt(filer, 10, "");
                return null;
            }
        });
        store.executeRewrite(key, newFilerInitialCapacity, new RewriteFilerTransaction<Filer, Void>() {
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

        store = new TxKeyedFilerStore(chunkStores,
            "booya".getBytes());

        store.execute(key, newFilerInitialCapacity, new FilerTransaction<Filer, Void>() {
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
