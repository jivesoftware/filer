package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 */
public class VariableKeySizeMapChunkBackedMapStoreTest {

    private String[] pathsToPartitions;

    @BeforeMethod
    public void setUp() throws Exception {
        pathsToPartitions = new String[] {
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath(),
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath()
        };
    }

    private VariableKeySizeMapChunkBackedMapStore<String, Long> createMapStore(String[] pathsToPartitions, int[] keySizeThresholds) throws Exception {

        VariableKeySizeMapChunkBackedMapStore.Builder<String, Long> builder = new VariableKeySizeMapChunkBackedMapStore.Builder<>(1,
            null,
            new KeyValueMarshaller<String, Long>() {

                @Override
                public byte[] keyBytes(String key) {
                    return key.getBytes(Charsets.US_ASCII);
                }

                @Override
                public byte[] valueBytes(Long value) {
                    return FilerIO.longBytes(value);
                }

                @Override
                public String bytesKey(byte[] bytes, int offset) {
                    return new String(bytes, offset, bytes.length - offset, Charsets.US_ASCII);
                }

                @Override
                public Long bytesValue(String key, byte[] bytes, int offset) {
                    return FilerIO.bytesLong(bytes, offset);
                }
            });
        for (int keySize : keySizeThresholds) {
            String[] keySizePaths = new String[pathsToPartitions.length];
            for (int i = 0; i < keySizePaths.length; i++) {
                keySizePaths[i] = pathsToPartitions[i] + File.separator + keySize;
            }
            builder.add(keySize, new FileBackedMapChunkFactory(keySize, true, 8, false, 2, keySizePaths), new KeyPartitioner<String>() {

                @Override
                public String keyPartition(String key) {
                    return "0";
                }

                @Override
                public Iterable<String> allPartitions() {
                    return Collections.singletonList("0");
                }

            });
        }
        return builder.build();
    }

    @Test
    public void testAddGet() throws Exception {
        int[] keySizeThresholds = new int[] { 4, 16, 64, 256, 1_024 };
        VariableKeySizeMapChunkBackedMapStore<String, Long> mapStore = createMapStore(pathsToPartitions, keySizeThresholds);

        for (int i = 0; i < keySizeThresholds.length; i++) {
            System.out.println("hmm:" + i);
            String key = keyOfLength(keySizeThresholds[i]);
            long expected = i;
            mapStore.add(key, expected);
            Assert.assertEquals(mapStore.get(key).longValue(), expected);
        }
    }

    @Test(expectedExceptions = { IndexOutOfBoundsException.class })
    public void testKeyTooBig() throws KeyValueStoreException, Exception {
        int[] keySizeThresholds = new int[] { 1, 2, 4 };
        VariableKeySizeMapChunkBackedMapStore<String, Long> mapStore = createMapStore(pathsToPartitions, keySizeThresholds);

        int maxLength = keySizeThresholds[keySizeThresholds.length - 1];
        mapStore.add(keyOfLength(maxLength + 1), 0l);
    }

    @Test(expectedExceptions = { IllegalStateException.class })
    public void testBadThresholds() throws KeyValueStoreException, Exception {
        int[] keySizeThresholds = new int[] { 0, 0 };
        createMapStore(pathsToPartitions, keySizeThresholds);
    }

    private String keyOfLength(int length) {
        StringBuilder buf = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            buf.append('a');
        }
        return buf.toString();
    }
}
