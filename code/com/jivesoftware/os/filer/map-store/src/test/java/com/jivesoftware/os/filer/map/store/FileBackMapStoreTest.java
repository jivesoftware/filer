package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Functions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.map.store.api.PartitionedKeyValueStore;
import java.nio.file.Files;
import java.util.Set;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

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
        FileBackMapStore<Integer, Long> fileBackMapStore = new FileBackMapStore<Integer, Long>(paths, 4, 8, 512, 4, null) {
            @Override
            public String keyPartition(Integer key) {
                return String.valueOf(key % 10);
            }

            @Override
            public Iterable<String> keyPartitions() {
                // so fancy
                return Iterables.transform(ContiguousSet.create(Range.closedOpen(0, 10), DiscreteDomain.integers()), Functions.toStringFunction());
            }

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
        };

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

        for (PartitionedKeyValueStore.Entry<Integer, Long> entry : fileBackMapStore) {
            actualKeys.add(entry.getKey());
            actualPayloads.add(entry.getValue());
        }

        assertEquals(actualKeys, expectedKeys);
        assertEquals(actualPayloads, expectedPayloads);
    }
}