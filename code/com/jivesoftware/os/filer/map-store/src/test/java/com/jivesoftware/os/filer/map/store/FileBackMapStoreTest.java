package com.jivesoftware.os.filer.map.store;

import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;
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
            final long payload = (long) i * numEntries;
            fileBackMapStore.execute(i, true, new KeyValueTransaction<Long, Void>() {
                @Override
                public Void commit(KeyValueContext<Long> context) throws IOException {
                    context.set(payload);
                    return null;
                }
            });

            expectedKeys.add(i);
            expectedPayloads.add(payload);
        }

        final Set<Integer> actualKeys = Sets.newTreeSet();
        final Set<Long> actualPayloads = Sets.newTreeSet();

        fileBackMapStore.stream(new KeyValueStore.EntryStream<Integer, Long>() {
            @Override
            public boolean stream(Integer key, Long value) throws IOException {
                actualKeys.add(key);
                actualPayloads.add(value);
                return true;
            }
        });

        assertEquals(actualKeys, expectedKeys);
        assertEquals(actualPayloads, expectedPayloads);
    }
}
