package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyPartitioner;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

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
            assertEquals(mapStore.get(key).longValue(), expected);
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

    @Test
    public void testIterator() throws Exception {
        int[] keySizeThresholds = new int[] { 4, 16 };
        VariableKeySizeMapChunkBackedMapStore<String, Long> mapStore = createMapStore(pathsToPartitions, keySizeThresholds);

        String keyspace = "abcdefghijklmnopqrstuvwxyz";
        for (int i = 1; i <= 16; i++) {
            mapStore.add(keyspace.substring(0, i), (long) i);
        }

        List<KeyValueStore.Entry<String, Long>> entries = Lists.newArrayList(mapStore.iterator());
        Collections.sort(entries, new Comparator<KeyValueStore.Entry<String, Long>>() {
            @Override
            public int compare(KeyValueStore.Entry<String, Long> o1, KeyValueStore.Entry<String, Long> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });
        int i = 1;
        for (KeyValueStore.Entry<String, Long> entry : entries) {
            System.out.println("key=" + entry.getKey() + " size=" + entry.getKey().length());
            System.out.println("value=" + entry.getValue());
            assertEquals(entry.getKey(), keyspace.substring(0, i));
            assertEquals(entry.getValue().longValue(), (long) i);
            i++;
        }
    }

    @Test
    public void testKeysIterator() throws Exception {
        int[] keySizeThresholds = new int[] { 4, 16 };
        VariableKeySizeMapChunkBackedMapStore<String, Long> mapStore = createMapStore(pathsToPartitions, keySizeThresholds);

        String keyspace = "abcdefghijklmnopqrstuvwxyz";
        for (int i = 1; i <= 16; i++) {
            mapStore.add(keyspace.substring(0, i), (long) i);
        }

        List<String> keys = Lists.newArrayList(mapStore.keysIterator());
        Collections.sort(keys, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return new Integer(o1.length()).compareTo(o2.length());
            }
        });
        int i = 1;
        for (String key : keys) {
            System.out.println("key=" + key + " size=" + key.length());
            assertEquals(key, keyspace.substring(0, i));
            i++;
        }
    }

    private String keyOfLength(int length) {
        StringBuilder buf = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            buf.append('a');
        }
        return buf.toString();
    }
}
