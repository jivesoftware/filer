package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.ByteBufferBackedConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.KeyValueMarshaller;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import java.io.IOException;
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
            @SuppressWarnings("unchecked")
            BytesBytesMapStore<ByteBufferBackedFiler, Long, Integer> mapStore =
                new BytesBytesMapStore<>(
                    8,
                    null,
                    new ConcurrentFilerProviderBackedMapChunkProvider<ByteBufferBackedFiler>(
                        8, false, 4, false, 10,
                        new ConcurrentFilerProvider[] {
                            new ConcurrentFilerProvider<>("booya".getBytes(Charsets.UTF_8),
                                new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory()))
                        }),
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
                    final int value = values[fieldId * numFields + termId];
                    mapStore.execute(key, true, new KeyValueTransaction<Integer, Void>() {
                        @Override
                        public Void commit(KeyValueContext<Integer> context) throws IOException {
                            context.set(value);
                            return null;
                        }
                    });
                }
            }

            // bytebuffer mapstore retrieve
            start = System.currentTimeMillis();
            for (int fieldId = 0; fieldId < numFields; fieldId++) {
                final int _fieldId = fieldId;
                for (int termId = 0; termId < numTerms; termId++) {
                    final int _termId = termId;
                    long key = (long) termId << 32 | fieldId & 0xFFFF_FFFFL;
                    final int expected = values[fieldId * numFields + termId];
                    mapStore.execute(key, true, new KeyValueTransaction<Integer, Void>() {
                        @Override
                        public Void commit(KeyValueContext<Integer> context) throws IOException {
                            Integer retrieved = context.get();
                            assertTrue(retrieved == expected, "Failed at " + _fieldId + ", " + _termId);
                            return null;
                        }
                    });
                }
            }

            if (i == numIterations - 1) {
                Thread.sleep(600_000);
            }

        }
    }
}
