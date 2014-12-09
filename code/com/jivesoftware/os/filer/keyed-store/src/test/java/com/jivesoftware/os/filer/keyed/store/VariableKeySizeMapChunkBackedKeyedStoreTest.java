package com.jivesoftware.os.filer.keyed.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStoreConcurrentFilerFactory;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.ConcurrentFilerProviderBackedMapChunkFactory;
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
public class VariableKeySizeMapChunkBackedKeyedStoreTest {

    private String[] mapDirectories;
    private String[] swapDirectories;
    private String[] chunkDirectories;

    @BeforeMethod
    public void setUp() throws Exception {
        mapDirectories = new String[] {
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath(),
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath()
        };
        swapDirectories = new String[] {
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath(),
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath()
        };
        chunkDirectories = new String[] {
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
        final int[] keySizeThresholds = new int[] { 4, 16, 64, 256, 1024 };
        int chunkStoreCapacityInBytes = 30 * 1024 * 1024;
        MultiChunkStore multChunkStore = new MultiChunkStoreInitializer(new ChunkStoreInitializer()).initializeMultiFileBacked(
            chunkDirectories, "data", 4, chunkStoreCapacityInBytes, false, 8, 64);
        long newFilerInitialCapacity = 512;
        VariableKeySizeMapChunkBackedKeyedStore.Builder<ByteBufferBackedFiler> builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder<>();

        for (int keySize : keySizeThresholds) {
            FileBackedMapChunkFactory mapChunkFactory = new FileBackedMapChunkFactory(keySize, true, 8, false, 512,
                buildMapDirectories(mapDirectories, keySize));
            FileBackedMapChunkFactory swapChunkFactory = new FileBackedMapChunkFactory(keySize, true, 8, false, 512,
                buildMapDirectories(swapDirectories, keySize));
            builder.add(keySize, new PartitionedMapChunkBackedKeyedStore<>(mapChunkFactory, swapChunkFactory, multChunkStore,
                new StripingLocksProvider<String>(16), 4));
        }

        VariableKeySizeMapChunkBackedKeyedStore keyedStore = builder.build();

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
        final int[] keySizeThresholds = new int[] { 4, 16 };
        int chunkStoreCapacityInBytes = 30 * 1024 * 1024;
        int newFilerInitialCapacity = 512;
        MultiChunkStore multChunkStore = new MultiChunkStoreInitializer(new ChunkStoreInitializer()).initializeMultiFileBacked(
            chunkDirectories, "data", 4, chunkStoreCapacityInBytes, false, 8, 64);
        VariableKeySizeMapChunkBackedKeyedStore.Builder<ByteBufferBackedFiler> builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder<>();

        for (int keySize : keySizeThresholds) {
            FileBackedMapChunkFactory mapChunkFactory = new FileBackedMapChunkFactory(keySize, true, 8, false, 512,
                buildMapDirectories(mapDirectories, keySize));
            FileBackedMapChunkFactory swapChunkFactory = new FileBackedMapChunkFactory(keySize, true, 8, false, 512,
                buildMapDirectories(swapDirectories, keySize));
            builder.add(keySize, new PartitionedMapChunkBackedKeyedStore<>(mapChunkFactory, swapChunkFactory, multChunkStore,
                new StripingLocksProvider<String>(16), 4));
        }

        VariableKeySizeMapChunkBackedKeyedStore keyedStore = builder.build();

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

    @Test
    public void testChunkBacked() throws Exception {
        final int[] keySizeThresholds = new int[] { 4, 16, 64, 256, 1024 };
        int chunkStoreCapacityInBytes = 30 * 1024 * 1024;

        File mapDir = Files.createTempDirectory("map").toFile();
        MultiChunkStoreInitializer mcsi = new MultiChunkStoreInitializer(new ChunkStoreInitializer());
        FileBackedMemMappedByteBufferFactory byteBufferFactory = new FileBackedMemMappedByteBufferFactory(mapDir);
        MultiChunkStoreConcurrentFilerFactory store = mcsi.initializeMultiByteBufferBacked(
            "boo", byteBufferFactory, 10, chunkStoreCapacityInBytes, true, 10, 10);

        long newFilerInitialCapacity = 512;
        VariableKeySizeMapChunkBackedKeyedStore.Builder<ChunkFiler> builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder<>();

        for (int keySize : keySizeThresholds) {
            ConcurrentFilerProviderBackedMapChunkFactory<ChunkFiler> mapChunkFactory = new ConcurrentFilerProviderBackedMapChunkFactory<>(
                keySize, true, 8, false, 512,
                new ConcurrentFilerProvider<>(("booya-map-" + keySize).getBytes(), store));
            ConcurrentFilerProviderBackedMapChunkFactory<ChunkFiler> swapChunkFactory = new ConcurrentFilerProviderBackedMapChunkFactory<>(
                keySize, true, 8, false, 512,
                new ConcurrentFilerProvider<>(("booya-swap-" + keySize).getBytes(), store));

            builder.add(keySize, new PartitionedMapChunkBackedKeyedStore<>(mapChunkFactory, swapChunkFactory, store,
                new StripingLocksProvider<String>(16), 4));
        }

        VariableKeySizeMapChunkBackedKeyedStore<ChunkFiler> keyedStore = builder.build();

        for (int keySize : keySizeThresholds) {
            try (SwappableFiler filer = keyedStore.get(keyOfLength(keySize), newFilerInitialCapacity)) {
                synchronized (filer.lock()) {
                    filer.seek(0);
                    for (int i = 0; i < 1_000; i++) {
                        FilerIO.writeInt(filer, keySize, "keySize");
                    }
                }
            }
        }

        for (int keySize : keySizeThresholds) {
            try (SwappableFiler filer = keyedStore.get(keyOfLength(keySize), -1)) {
                synchronized (filer.lock()) {
                    filer.seek(0);
                    for (int i = 0; i < 1_000; i++) {
                        int actual = FilerIO.readInt(filer, "keySize");
                        Assert.assertEquals(actual, keySize);
                    }
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
