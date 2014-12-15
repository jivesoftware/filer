package com.jivesoftware.os.filer.map.store;

import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import java.nio.file.Files;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class FileBackMapStoreTest {

    @Test
    public void testIterator() throws Exception {
        testIteratorWithNumEntries(8_193);
    }

    private void testIteratorWithNumEntries(int numEntries) throws Exception {
        String[] paths = new String[] {
            Files.createTempDirectory("testIterator").toFile().getAbsolutePath(),
            Files.createTempDirectory("testIterator").toFile().getAbsolutePath()
        };
        FileBackedMapChunkProvider mapChunkFactory = new FileBackedMapChunkProvider(4, false, 8, false, 512, paths, 4);
        PartitionedMapChunkBackedMapStore<ByteBufferBackedFiler, Integer, Long> fileBackMapStore =
            new PartitionedMapChunkBackedMapStore<>(
                mapChunkFactory,
                null,
                new KeyValueMarshaller<Integer, Long>() {

                    @Override
                    public byte[] keyBytes(Integer key) {
                        return FilerIO.intBytes(key);
                    }

                    @Override
                    public byte[] valueBytes(Long value) {
                        return FilerIO.longBytes(value);
                    }

                    @Override
                    public Integer bytesKey(byte[] bytes, int offset) {
                        return FilerIO.bytesInt(bytes, offset);
                    }

                    @Override
                    public Long bytesValue(Integer key, byte[] bytes, int offset) {
                        return FilerIO.bytesLong(bytes, offset);
                    }
                });

        Set<Integer> expectedKeys = Sets.newTreeSet();
        Set<Long> expectedPayloads = Sets.newTreeSet();

        for (int i = 0; i < numEntries; i++) {
            long payload = (long) i * numEntries;
            fileBackMapStore.add(i, payload);

            expectedKeys.add(i);
            expectedPayloads.add(payload);
        }

        Set<Integer> actualKeys = Sets.newTreeSet();
        Set<Long> actualPayloads = Sets.newTreeSet();

        for (KeyValueStore.Entry<Integer, Long> entry : fileBackMapStore) {
            actualKeys.add(entry.getKey());
            actualPayloads.add(entry.getValue());
        }

        assertEquals(actualKeys, expectedKeys);
        assertEquals(actualPayloads, expectedPayloads);
    }
}
