package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.filer.io.map.SkipListMapContext;
import com.jivesoftware.os.filer.io.map.SkipListMapStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author jonathan
 */
public class SkipListMapTest {

    @Test
    public void addToCapactiyTest() throws IOException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 96;
        int keySize = 1;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, stackBuffer);

        for (int i = 0; i < 96; i++) {
            sls.add(f, from, new byte[]{(byte) i}, new byte[0], stackBuffer);
        }
    }

    @Test
    public void addHeadKeyTest() throws IOException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 96;
        int keySize = 4;
        int payloadSize = 4;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, stackBuffer);

        for (int i = 0; i < 10; i++) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), stackBuffer);
        }

        sls.add(f, from, headKey, FilerIO.intBytes(Integer.MIN_VALUE), stackBuffer);

        for (int i = 10; i < 20; i++) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), stackBuffer);
        }

        sls.add(f, from, headKey, FilerIO.intBytes(Integer.MIN_VALUE + 1), stackBuffer);

        for (int i = 20; i < 30; i++) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), stackBuffer);
        }

        sls.add(f, from, headKey, FilerIO.intBytes(Integer.MIN_VALUE + 2), stackBuffer);

        for (int i = 30; i < 40; i++) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), stackBuffer);
        }

        for (int i = 0; i < 40; i++) {
            assertEquals(sls.getExistingPayload(f, from, FilerIO.intBytes(i), stackBuffer), FilerIO.intBytes(Integer.MAX_VALUE - i));
        }

        assertEquals(sls.getExistingPayload(f, from, headKey, stackBuffer), FilerIO.intBytes(Integer.MIN_VALUE + 2));
    }

    @Test
    public void addAscending() throws IOException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 96;
        int keySize = 4;
        int payloadSize = 4;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, stackBuffer);

        for (int i = 0; i < capacity; i++) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), stackBuffer);
        }

        for (int i = 0; i < capacity; i++) {
            assertEquals(sls.getExistingPayload(f, from, FilerIO.intBytes(i), stackBuffer), FilerIO.intBytes(Integer.MAX_VALUE - i));
        }
    }

    @Test
    public void addDescending() throws IOException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 96;
        int keySize = 4;
        int payloadSize = 4;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, stackBuffer);

        for (int i = capacity; i > 0; i--) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), stackBuffer);
        }

        for (int i = capacity; i > 0; i--) {
            assertEquals(sls.getExistingPayload(f, from, FilerIO.intBytes(i), stackBuffer), FilerIO.intBytes(Integer.MAX_VALUE - i));
        }
    }

    @Test
    public void addRandom() throws IOException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 96;
        int keySize = 4;
        int payloadSize = 4;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, stackBuffer);

        int[] keys = new int[capacity];
        Random rand = new Random();
        for (int i = 0; i < capacity; i++) {
            keys[i] = rand.nextInt();
        }

        for (int i = 0; i < capacity; i++) {
            sls.add(f, from, FilerIO.intBytes(keys[i]), FilerIO.intBytes(Integer.MAX_VALUE - i), stackBuffer);
        }

        for (int i = 0; i < capacity; i++) {
            assertEquals(sls.getExistingPayload(f, from, FilerIO.intBytes(keys[i]), stackBuffer), FilerIO.intBytes(Integer.MAX_VALUE - i));
        }
    }

    @Test
    public void reopenTest() throws IOException, InterruptedException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 16;
        int keySize = 1;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, stackBuffer);

        final HashSet<Byte> expectedContains = new HashSet<Byte>();
        for (int i = 0; i < 10; i++) {
            sls.add(f, from, new byte[]{(byte) i}, new byte[0], stackBuffer);
            expectedContains.add((byte) i);
        }

        assertContents(sls, f, from, new HashSet<>(expectedContains), stackBuffer);

        SkipListMapContext reopen = sls.open(headKey, LexSkipListComparator.cSingleton, f, stackBuffer);
        assertContents(sls, f, reopen, new HashSet<>(expectedContains), stackBuffer);

    }

    @Test
    public void getsTest() throws IOException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 16;
        int keySize = 1;
        int payloadSize = 1;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, stackBuffer);

        for (int i = 0; i < 10; i++) {
            sls.add(f, from, new byte[]{(byte) i}, new byte[]{(byte) i}, stackBuffer);
        }

        assertEquals(10, sls.getCount(f, from, stackBuffer));

        assertEquals(new byte[]{0}, sls.getFirst(f, from, stackBuffer));
        assertEquals(new byte[]{1}, sls.getNextKey(f, from, new byte[]{0}, stackBuffer));
        assertEquals(new byte[]{0}, sls.getPrior(f, from, new byte[]{1}, stackBuffer));

        assertEquals(null, sls.getPrior(f, from, new byte[]{0}, stackBuffer));
        assertEquals(null, sls.getNextKey(f, from, new byte[]{9}, stackBuffer));

        assertEquals(new byte[]{9}, sls.getExistingPayload(f, from, new byte[]{9}, stackBuffer));
        assertEquals(null, sls.getExistingPayload(f, from, new byte[]{11}, stackBuffer));

    }

    @Test
    public void removeTest() throws IOException, InterruptedException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 16;
        int keySize = 1;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, stackBuffer);

        final HashSet<Byte> expectedContains = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            sls.add(f, from, new byte[]{(byte) i}, new byte[0], stackBuffer);
            expectedContains.add((byte) i);
        }

        assertContents(sls, f, from, new HashSet<>(expectedContains), stackBuffer);

        // remove from middle
        sls.remove(f, from, new byte[]{5}, stackBuffer);
        expectedContains.remove((byte) 5);

        assertContents(sls, f, from, new HashSet<>(expectedContains), stackBuffer);

        // remove first
        sls.remove(f, from, new byte[]{0}, stackBuffer);
        expectedContains.remove((byte) 0);

        assertContents(sls, f, from, new HashSet<>(expectedContains), stackBuffer);

        // remove last
        sls.remove(f, from, new byte[]{9}, stackBuffer);
        expectedContains.remove((byte) 9);

        assertContents(sls, f, from, new HashSet<>(expectedContains), stackBuffer);

        // remove second from last
        sls.remove(f, from, new byte[]{7}, stackBuffer);
        expectedContains.remove((byte) 7);

        assertContents(sls, f, from, new HashSet<>(expectedContains), stackBuffer);

        // remove second from first
        sls.remove(f, from, new byte[]{2}, stackBuffer);
        expectedContains.remove((byte) 2);

        assertContents(sls, f, from, new HashSet<>(expectedContains), stackBuffer);

    }

    private void assertContents(SkipListMapStore sls, ByteBufferBackedFiler f, SkipListMapContext from, final HashSet<Byte> expectedContains,
        StackBuffer stackBuffer)
        throws IOException, InterruptedException {
        sls.streamKeys(f, from, new Object(), null, key -> {
            if (key != null) {
                Assert.assertTrue(expectedContains.contains(key[0]), "Expected:" + key[0]);
                expectedContains.remove(key[0]);
            }
            return true;
        }, stackBuffer);
        Assert.assertTrue(expectedContains.isEmpty());
    }

    @Test
    public void copyToTest() throws IOException, InterruptedException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 16;
        int keySize = 1;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        Random random = new Random(1234);
        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, stackBuffer);
        final HashSet<Byte> expectedContains = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            byte[] key = new byte[]{(byte) random.nextInt(64)};
            System.out.println("add:" + Arrays.toString(key));
            sls.add(f, from, key, new byte[0], stackBuffer);
            expectedContains.add(key[0]);
        }

        capacity = 64;
        slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler t = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        SkipListMapContext to = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, t, stackBuffer);
        sls.copyTo(f, from, t, to, (fromIndex, toIndex) -> System.out.println(fromIndex + " " + toIndex), stackBuffer);

        sls.toSysOut(t, to, new SkipListMapStore.BytesToBytesString(), stackBuffer);
        assertContents(sls, t, to, expectedContains, stackBuffer);
    }

    @Test
    public void variableKeysTest() throws IOException, InterruptedException {
        StackBuffer stackBuffer = new StackBuffer();
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
        SkipListMapContext context = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, filer,
            stackBuffer);

        for (int i = 0; i < 2; i++) {
            byte[] key = new byte[1 + random.nextInt(keySize - 1)];
            random.nextBytes(key);
            System.out.println("add:" + Arrays.toString(key));
            sls.add(filer, context, key, new byte[0], stackBuffer);
        }

        sls.toSysOut(filer, context, new SkipListMapStore.BytesToBytesString() {
            @Override
            public String bytesToString(byte[] bytes) {
                return Arrays.toString(bytes);
            }
        }, stackBuffer);

        sls.streamKeys(filer, context, new Object(), null, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            return true;
        }, stackBuffer);

    }

    @Test
    public void streamRangesTest() throws IOException, InterruptedException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 1024;
        int keySize = 4;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, false, payloadSize, (byte) 9);
        ByteBufferBackedFiler filer = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext context = sls.create(capacity, headKey, keySize, false, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, filer,
            stackBuffer);

        for (int i = 0; i < 200; i++) {
            sls.add(filer, context, FilerIO.intBytes(i), new byte[0], stackBuffer);
        }

        List<KeyRange> ranges = new ArrayList<>();
        ranges.add(new KeyRange(FilerIO.intBytes(10), FilerIO.intBytes(21)));
        final AtomicInteger count = new AtomicInteger();
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, stackBuffer);
        assertEquals(count.intValue(), 11);

    }

    @Test
    public void prefixStreamTest() throws IOException, InterruptedException {
        StackBuffer stackBuffer = new StackBuffer();
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 14;
        int keySize = 2;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler filer = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext context = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, filer,
            stackBuffer);

        Random random = new Random(1234);
        for (int i = 0; i < 10; i++) {
            byte[] key = new byte[2];
            random.nextBytes(key);
            System.out.println("add:" + Arrays.toString(key));
            sls.add(filer, context, key, new byte[0], stackBuffer);
        }

        sls.toSysOut(filer, context, new SkipListMapStore.BytesToBytesString(), stackBuffer);

        final AtomicInteger count = new AtomicInteger();

        List<KeyRange> ranges = new ArrayList<>();
        ranges.add(new KeyRange(new byte[]{19, -5}, new byte[]{50}));
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, stackBuffer);
        assertEquals(count.intValue(), 1);

        System.out.println("-------");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{19}, new byte[]{50, -126}));
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, stackBuffer);
        assertEquals(count.intValue(), 3);

        System.out.println("-------");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{-11}, new byte[]{-4}));
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, stackBuffer);
        assertEquals(count.intValue(), 2);

        System.out.println("-------");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{-11}, new byte[]{-5}));
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, stackBuffer);
        assertEquals(count.intValue(), 1);

        System.out.println("------- -2, -1");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{-2}, new byte[]{-1}));
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, stackBuffer);
        assertEquals(count.intValue(), 0);

    }

    @Test
    public void slsSortRandDoublesTest() throws IOException, Exception {
        StackBuffer stackBuffer = new StackBuffer();
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
            SkipListMapContext page = sls.create(capacity, headKey, keySize, false, payloadSize, (byte) 9, DoubleSkipListComparator.cSingleton, filer,
                stackBuffer);
            for (int i = 0; i < insert; i++) {
                byte[] doubleBytes = FilerIO.doubleBytes(random.nextInt(range));
                //System.out.println(i + " Added:" + FilerIO.bytesDouble(doubleBytes));
                sls.add(filer, page, doubleBytes, new byte[0], stackBuffer);
            }

            System.out.println("\n Count:" + MapStore.INSTANCE.getCount(filer, stackBuffer));

            System.out.println("\nPrint sls:");
            SkipListMapStore.BytesToString toStringer = new SkipListMapStore.BytesToDoubleString();
            sls.toSysOut(filer, page, toStringer, stackBuffer);

            byte[] find = FilerIO.doubleBytes(random.nextInt(range));
            byte[] got = sls.findWouldInsertAtOrAfter(filer, page, find, stackBuffer);
            System.out.println("\n" + FilerIO.byteDouble(find) + " would be inserted at or after " + FilerIO.byteDouble(got));

        }

    }
}
