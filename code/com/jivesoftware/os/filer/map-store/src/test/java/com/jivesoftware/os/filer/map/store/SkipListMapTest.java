package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.map.store.api.KeyRange;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class SkipListMapTest {

    @Test
    public void variableKeysTest() throws IOException {
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 1024;
        int keySize = 4;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler filer = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        Random random = new Random(1234);
        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext context = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, filer);

        for (int i = 0; i < 2; i++) {
            byte[] key = new byte[1 + random.nextInt(keySize - 1)];
            random.nextBytes(key);
            System.out.println("add:" + Arrays.toString(key));
            sls.add(filer, context, key, new byte[0]);
        }

        sls.toSysOut(filer, context, new SkipListMapStore.BytesToBytesString() {
            @Override
            public String bytesToString(byte[] bytes) {
                return Arrays.toString(bytes);
            }
        });

        sls.streamKeys(filer, context, new Object(), null, new MapStore.KeyStream() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                System.out.println("stream:" + Arrays.toString(key));
                return true;
            }
        });

    }

    @Test
    public void streamRangesTest() throws IOException {
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 1024;
        int keySize = 4;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, false, payloadSize, (byte) 9);
        ByteBufferBackedFiler filer = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext context = sls.create(capacity, headKey, keySize, false, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, filer);

        for (int i = 0; i < 200; i++) {
            sls.add(filer, context, FilerIO.intBytes(i), new byte[0]);
        }

        List<KeyRange> ranges = new ArrayList<>();
        ranges.add(new KeyRange(FilerIO.intBytes(10), FilerIO.intBytes(21)));
        final AtomicInteger count = new AtomicInteger();
        sls.streamKeys(filer, context, new Object(), ranges, new MapStore.KeyStream() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                System.out.println("stream:" + Arrays.toString(key));
                count.incrementAndGet();
                return true;
            }
        });
        Assert.assertEquals(count.intValue(), 11);

    }

     @Test
    public void prefixStreamTest() throws IOException {
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 14;
        int keySize = 2;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler filer = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext context = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, filer);

        Random random = new Random(1234);
        for (int i = 0; i < 10; i++) {
            byte[] key = new byte[2];
            random.nextBytes(key);
            System.out.println("add:" + Arrays.toString(key));
            sls.add(filer, context, key, new byte[0]);
        }

        sls.toSysOut(filer, context, new SkipListMapStore.BytesToBytesString());

        final AtomicInteger count = new AtomicInteger();

        List<KeyRange> ranges = new ArrayList<>();
        ranges.add(new KeyRange(new byte[]{19, -5}, new byte[]{50}));
        sls.streamKeys(filer, context, new Object(), ranges, new MapStore.KeyStream() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                System.out.println("stream:" + Arrays.toString(key));
                count.incrementAndGet();
                return true;
            }
        });
        Assert.assertEquals(count.intValue(), 1);

        System.out.println("-------");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{19}, new byte[]{50, -126}));
        sls.streamKeys(filer, context, new Object(), ranges, new MapStore.KeyStream() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                System.out.println("stream:" + Arrays.toString(key));
                count.incrementAndGet();
                return true;
            }
        });
        Assert.assertEquals(count.intValue(), 3);

        System.out.println("-------");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{-11}, new byte[]{-4}));
        sls.streamKeys(filer, context, new Object(), ranges, new MapStore.KeyStream() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                System.out.println("stream:" + Arrays.toString(key));
                count.incrementAndGet();
                return true;
            }
        });
        Assert.assertEquals(count.intValue(), 2);

        System.out.println("-------");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{-11}, new byte[]{-5}));
        sls.streamKeys(filer, context, new Object(), ranges, new MapStore.KeyStream() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                System.out.println("stream:" + Arrays.toString(key));
                count.incrementAndGet();
                return true;
            }
        });
        Assert.assertEquals(count.intValue(), 1);

        System.out.println("------- -2, -1");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{-2}, new byte[]{-1}));
        sls.streamKeys(filer, context, new Object(), ranges, new MapStore.KeyStream() {

            @Override
            public boolean stream(byte[] key) throws IOException {
                System.out.println("stream:" + Arrays.toString(key));
                count.incrementAndGet();
                return true;
            }
        });
        Assert.assertEquals(count.intValue(), 0);

    }

    @Test
    public void slsSortRandDoublesTest() throws IOException, Exception {
        for (int t = 0; t < 1; t++) {

            int capacity = 200_000;
            int keySize = 8;
            int payloadSize = 0;
            int range = 256;
            int insert = 128;
            ByteBufferFactory provider = new HeapByteBufferFactory();

            SkipListMapStore sls = SkipListMapStore.INSTANCE;
            long slsFilerSize = sls.computeFilerSize(capacity, keySize, false, payloadSize, (byte) 9);
            ByteBufferBackedFiler filer = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

            Random random = new Random(1234);
            byte[] headKey = new byte[keySize];
            Arrays.fill(headKey, Byte.MIN_VALUE);
            SkipListMapContext page = sls.create(capacity, headKey, keySize, false, payloadSize, (byte) 9, DoubleSkipListComparator.cSingleton, filer);
            for (int i = 0; i < insert; i++) {
                byte[] doubleBytes = FilerIO.doubleBytes(random.nextInt(range));
                //System.out.println(i + " Added:" + FilerIO.bytesDouble(doubleBytes));
                sls.add(filer, page, doubleBytes, new byte[0]);
            }

            System.out.println("\n Count:" + MapStore.INSTANCE.getCount(filer));

            System.out.println("\nPrint sls:");
            SkipListMapStore.BytesToString toStringer = new SkipListMapStore.BytesToDoubleString();
            sls.toSysOut(filer, page, toStringer);

            byte[] find = FilerIO.doubleBytes(random.nextInt(range));
            byte[] got = sls.findWouldInsertAtOrAfter(filer, page, find);
            System.out.println("\n" + FilerIO.byteDouble(find) + " would be inserted at or after " + FilerIO.byteDouble(got));

            final AtomicInteger i = new AtomicInteger(1);
            System.out.println("\nRange from " + FilerIO.byteDouble(find) + " max after 4");
            sls.getSlice(filer, page, find, null, 4, new SkipListMapStore.SliceStream() {

                @Override
                public KeyPayload stream(KeyPayload v) throws IOException {
                    if (v != null) {
                        System.out.println(i + ":" + FilerIO.byteDouble(v.key));
                        i.incrementAndGet();
                    }
                    return v;
                }
            });

            i.set(1);
            double from = random.nextInt(3);
            double to = random.nextInt(6 + (range - 6));
            System.out.println("\nRange " + from + " -> " + to);
            sls.getSlice(filer, page, FilerIO.doubleBytes(from), FilerIO.doubleBytes(from + to), -1,
                new SkipListMapStore.SliceStream() {

                    @Override
                    public KeyPayload stream(KeyPayload v) throws IOException {
                        if (v != null) {
                            System.out.println(i + ":" + FilerIO.byteDouble(v.key)); //toStringer.bytesToString(v.key));
                            i.incrementAndGet();
                        }
                        return v;
                    }
                });
        }

    }
}
