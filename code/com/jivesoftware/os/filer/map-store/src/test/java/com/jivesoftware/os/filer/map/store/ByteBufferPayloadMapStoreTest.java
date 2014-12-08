package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.ByteBufferBackedConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class ByteBufferPayloadMapStoreTest {

    @Test(enabled = false)
    public void testPrimitiveArrays() throws Exception {
        final int numFields = 10;
        final int numTerms = 100_000;
        final int numIterations = 20;
        int[] values = new int[numFields * numTerms];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }

        for (int i = 0; i < numIterations; i++) {
            System.out.println("---------------------- " + i + " ----------------------");

            // bytebuffer mapstore setup
            BytesBytesMapStore<Long, Integer> mapStore =
                new BytesBytesMapStore<>("8",
                    8,
                    null,
                    new ConcurrentFilerProviderBackedMapChunkFactory<>(8, false, 4, false, 10,
                        new ConcurrentFilerProvider<>("booya".getBytes(Charsets.UTF_8),
                            new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory()))),
                    new KeyValueMarshaller<Long, Integer>() {

                        @Override
                        public byte[] keyBytes(Long key) {
                            return FilerIO.longBytes(key);
                        }

                        @Override
                        public Long bytesKey(byte[] bytes, int offset) {
                            return FilerIO.bytesLong(bytes, offset);
                        }

                        @Override
                        public byte[] valueBytes(Integer value) {
                            return FilerIO.intBytes(value);
                        }

                        @Override
                        public Integer bytesValue(Long key, byte[] bytes, int offset) {
                            return FilerIO.bytesInt(bytes, offset);
                        }
                    });

            // bytebuffer mapstore insert
            long start = System.currentTimeMillis();
            for (int fieldId = 0; fieldId < numFields; fieldId++) {
                for (int termId = 0; termId < numTerms; termId++) {
                    long key = (long) termId << 32 | fieldId & 0xFFFF_FFFFL;
                    mapStore.add(key, values[fieldId * numFields + termId]);
                }
            }

            // bytebuffer mapstore retrieve
            start = System.currentTimeMillis();
            for (int fieldId = 0; fieldId < numFields; fieldId++) {
                for (int termId = 0; termId < numTerms; termId++) {
                    long key = (long) termId << 32 | fieldId & 0xFFFF_FFFFL;
                    Integer retrieved = mapStore.get(key);
                    assertTrue(retrieved == values[fieldId * numFields + termId], "Failed at " + fieldId + ", " + termId);
                }
            }

            if (i == numIterations - 1) {
                Thread.sleep(600_000);
            }

        }
    }
}
