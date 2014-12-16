package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
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

    private VariableKeySizeMapChunkBackedMapStore<ByteBufferBackedFiler, String, Long> createMapStore(
        String[] pathsToPartitions,
        int[] keySizeThresholds)
        throws Exception {

        VariableKeySizeMapChunkBackedMapStore.Builder<ByteBufferBackedFiler, String, Long> builder =
            new VariableKeySizeMapChunkBackedMapStore.Builder<>(
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
            builder.add(keySize, new FileBackedMapChunkProvider(keySize, true, 8, false, 2, keySizePaths, 4));
        }
        return builder.build();
    }

    @Test
    public void testAddGet() throws Exception {
        int[] keySizeThresholds = new int[] { 4, 16, 64, 256, 1_024 };
        VariableKeySizeMapChunkBackedMapStore<ByteBufferBackedFiler, String, Long> mapStore =
            createMapStore(pathsToPartitions, keySizeThresholds);

        for (int i = 0; i < keySizeThresholds.length; i++) {
            System.out.println("hmm:" + i);
            String key = keyOfLength(keySizeThresholds[i]);
            final long expected = i;
            mapStore.execute(key, true, new KeyValueTransaction<Long, Void>() {
                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    context.set(expected);
                    assertEquals(context.get().longValue(), expected);
                    return null;
                }
            });
        }
    }

    @Test(expectedExceptions = { IndexOutOfBoundsException.class })
    public void testKeyTooBig() throws Exception {
        int[] keySizeThresholds = new int[] { 1, 2, 4 };
        VariableKeySizeMapChunkBackedMapStore<ByteBufferBackedFiler, String, Long> mapStore =
            createMapStore(pathsToPartitions, keySizeThresholds);

        int maxLength = keySizeThresholds[keySizeThresholds.length - 1];
        mapStore.execute(keyOfLength(maxLength + 1), true, new KeyValueTransaction<Long, Void>() {
            @Override
            public Void commit(KeyValueContext<Long> context) throws IOException {
                context.set(0L);
                return null;
            }
        });
    }

    @Test(expectedExceptions = { IllegalStateException.class })
    public void testBadThresholds() throws KeyValueStoreException, Exception {
        int[] keySizeThresholds = new int[] { 0, 0 };
        createMapStore(pathsToPartitions, keySizeThresholds);
    }

    @Test
    public void testStream() throws Exception {
        int[] keySizeThresholds = new int[] { 4, 16 };
        VariableKeySizeMapChunkBackedMapStore<ByteBufferBackedFiler, String, Long> mapStore =
            createMapStore(pathsToPartitions, keySizeThresholds);

        final String keyspace = "abcdefghijklmnopqrstuvwxyz";
        for (int i = 1; i <= 16; i++) {
            final long value = (long) i;
            mapStore.execute(keyspace.substring(0, i), true, new KeyValueTransaction<Long, Void>() {
                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    context.set(value);
                    return null;
                }
            });
        }

        final AtomicInteger keyLength = new AtomicInteger(1);
        mapStore.stream(new KeyValueStore.EntryStream<String, Long>() {
            @Override
            public boolean stream(String key, Long value) throws IOException {
                System.out.println("key=" + key + " size=" + key.length());
                System.out.println("value=" + value);
                int _keyLength = keyLength.get();
                assertEquals(key, keyspace.substring(0, _keyLength));
                assertEquals(value.longValue(), (long) _keyLength);
                keyLength.incrementAndGet();
                return true;
            }
        });
    }

    @Test
    public void testKeysStream() throws Exception {
        int[] keySizeThresholds = new int[] { 4, 16 };
        VariableKeySizeMapChunkBackedMapStore<ByteBufferBackedFiler, String, Long> mapStore =
            createMapStore(pathsToPartitions, keySizeThresholds);

        final String keyspace = "abcdefghijklmnopqrstuvwxyz";
        for (int i = 1; i <= 16; i++) {
            final long value = (long) i;
            mapStore.execute(keyspace.substring(0, i), true, new KeyValueTransaction<Long, Void>() {
                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    context.set(value);
                    return null;
                }
            });
        }

        final AtomicInteger keyLength = new AtomicInteger(1);
        mapStore.streamKeys(new KeyValueStore.KeyStream<String>() {
            @Override
            public boolean stream(String key) throws IOException {
                System.out.println("key=" + key + " size=" + key.length());
                int _keyLength = keyLength.get();
                assertEquals(key, keyspace.substring(0, _keyLength));
                keyLength.incrementAndGet();
                return true;
            }
        });
    }

    private String keyOfLength(int length) {
        StringBuilder buf = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            buf.append('a');
        }
        return buf.toString();
    }
}
