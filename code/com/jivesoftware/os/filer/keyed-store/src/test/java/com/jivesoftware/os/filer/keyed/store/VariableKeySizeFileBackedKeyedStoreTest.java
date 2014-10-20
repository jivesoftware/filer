package com.jivesoftware.os.filer.keyed.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import java.io.File;
import java.nio.file.Files;
import org.apache.commons.math.util.MathUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 */
public class VariableKeySizeFileBackedKeyedStoreTest {

    private String[] mapDirectories;
    private String[] swapDirectories;
    private String[] chunkDirectories;

    @BeforeMethod
    public void setUp() throws Exception {
        mapDirectories = new String[]{
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath(),
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath()
        };
        swapDirectories = new String[]{
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath(),
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath()
        };
        chunkDirectories = new String[]{
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath(),
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath()
        };
    }

    private String[] buildMapDirectories(String[] baseMapDirectories, int keySize) {
        String[] mapDirectories = new String[baseMapDirectories.length];
        for (int basePathIndex = 0; basePathIndex < baseMapDirectories.length; basePathIndex++) {
            mapDirectories[basePathIndex] = new File(baseMapDirectories[basePathIndex], String.valueOf(keySize)).getAbsolutePath();
        }
        return mapDirectories;
    }

    @Test
    public void testFilerAutoCreate() throws Exception {
        final int[] keySizeThresholds = new int[]{4, 16, 64, 256, 1024};
        int chunkStoreCapacityInBytes = 30 * 1024 * 1024;
        MultiChunkStore multChunkStore = new ChunkStoreInitializer().initializeMulti(chunkDirectories, "data", 4, chunkStoreCapacityInBytes, false);
        long newFilerInitialCapacity = 512;
        VariableKeySizeFileBackedKeyedStore.Builder builder = new VariableKeySizeFileBackedKeyedStore.Builder();

        for (int keySize : keySizeThresholds) {
            FileBackedMapChunkFactory mapChunkFactory = new FileBackedMapChunkFactory(keySize, true, 8, false, 512, buildMapDirectories(mapDirectories, keySize));
            FileBackedMapChunkFactory swapChunkFactory = new FileBackedMapChunkFactory(keySize, true, 8, false, 512, buildMapDirectories(swapDirectories, keySize));

            builder.add(keySize, new FileBackedKeyedStore(mapChunkFactory, swapChunkFactory, multChunkStore, 4));

        }

        VariableKeySizeFileBackedKeyedStore keyedStore = builder.build();

        for (int keySize : keySizeThresholds) {
            Filer filer = keyedStore.get(keyOfLength(keySize), newFilerInitialCapacity);
            synchronized (filer.lock()) {
                filer.seek(0);
                FilerIO.writeInt(filer, keySize, "keySize");
            }
        }

        for (int keySize : keySizeThresholds) {
            Filer filer = keyedStore.get(keyOfLength(keySize), -1);
            synchronized (filer.lock()) {
                filer.seek(0);
                int actual = FilerIO.readInt(filer, "keySize");
                Assert.assertEquals(actual, keySize);
            }
        }
    }

    @Test
    public void testFilerGrowsCapacity() throws Exception {
        final int[] keySizeThresholds = new int[]{4, 16};
        int chunkStoreCapacityInBytes = 30 * 1024 * 1024;
        int newFilerInitialCapacity = 512;
        MultiChunkStore multChunkStore = new ChunkStoreInitializer().initializeMulti(chunkDirectories, "data", 4, chunkStoreCapacityInBytes, false);
        VariableKeySizeFileBackedKeyedStore.Builder builder = new VariableKeySizeFileBackedKeyedStore.Builder();

        for (int keySize : keySizeThresholds) {
            FileBackedMapChunkFactory mapChunkFactory = new FileBackedMapChunkFactory(keySize, true, 8, false, 512, buildMapDirectories(mapDirectories, keySize));
            FileBackedMapChunkFactory swapChunkFactory = new FileBackedMapChunkFactory(keySize, true, 8, false, 512, buildMapDirectories(swapDirectories, keySize));

            builder.add(keySize, new FileBackedKeyedStore(mapChunkFactory, swapChunkFactory, multChunkStore, 4));

        }

        VariableKeySizeFileBackedKeyedStore keyedStore = builder.build();

        int numberOfIntsInInitialCapacity = newFilerInitialCapacity / 4;
        int numberOfIntsInActualCapacity = numberOfIntsInInitialCapacity * 2; // actual capacity is doubled
        int numberOfTimesToGrow = 3;
        int totalNumberOfInts = numberOfIntsInActualCapacity * MathUtils.pow(2, numberOfTimesToGrow - 1);

        for (int keySize : keySizeThresholds) {
            Filer filer = keyedStore.get(keyOfLength(keySize), newFilerInitialCapacity);
            synchronized (filer.lock()) {
                filer.seek(0);
                for (int i = 0; i < totalNumberOfInts; i++) {
                    FilerIO.writeInt(filer, i, String.valueOf(i));
                }
            }
        }

        for (int keySize : keySizeThresholds) {
            Filer filer = keyedStore.get(keyOfLength(keySize), newFilerInitialCapacity);
            synchronized (filer.lock()) {
                filer.seek(0);
                for (int i = 0; i < totalNumberOfInts; i++) {
                    int actual = FilerIO.readInt(filer, String.valueOf(i));
                    Assert.assertEquals(actual, i);
                }
            }
        }
    }

    private byte[] keyOfLength(int length) {
        StringBuilder buf = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            buf.append('a');
        }
        return buf.toString().getBytes(Charsets.US_ASCII);
    }
}
