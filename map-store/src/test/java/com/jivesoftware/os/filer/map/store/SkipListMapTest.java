package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.api.KeyRange;
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
        byte[] primitiveBuffer = new byte[8];
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 96;
        int keySize = 1;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, primitiveBuffer);

        for (int i = 0; i < 96; i++) {
            sls.add(f, from, new byte[]{(byte) i}, new byte[0], primitiveBuffer);
        }
    }

    @Test
    public void addHeadKeyTest() throws IOException {
        byte[] primitiveBuffer = new byte[8];
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 96;
        int keySize = 4;
        int payloadSize = 4;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, primitiveBuffer);

        for (int i = 0; i < 10; i++) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), primitiveBuffer);
        }

        sls.add(f, from, headKey, FilerIO.intBytes(Integer.MIN_VALUE), primitiveBuffer);

        for (int i = 10; i < 20; i++) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), primitiveBuffer);
        }

        sls.add(f, from, headKey, FilerIO.intBytes(Integer.MIN_VALUE + 1), primitiveBuffer);

        for (int i = 20; i < 30; i++) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), primitiveBuffer);
        }

        sls.add(f, from, headKey, FilerIO.intBytes(Integer.MIN_VALUE + 2), primitiveBuffer);

        for (int i = 30; i < 40; i++) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), primitiveBuffer);
        }

        for (int i = 0; i < 40; i++) {
            assertEquals(sls.getExistingPayload(f, from, FilerIO.intBytes(i), primitiveBuffer), FilerIO.intBytes(Integer.MAX_VALUE - i));
        }

        assertEquals(sls.getExistingPayload(f, from, headKey, primitiveBuffer), FilerIO.intBytes(Integer.MIN_VALUE + 2));
    }

    @Test
    public void addAscending() throws IOException {
        byte[] primitiveBuffer = new byte[8];
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 96;
        int keySize = 4;
        int payloadSize = 4;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, primitiveBuffer);

        for (int i = 0; i < capacity; i++) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), primitiveBuffer);
        }

        for (int i = 0; i < capacity; i++) {
            assertEquals(sls.getExistingPayload(f, from, FilerIO.intBytes(i), primitiveBuffer), FilerIO.intBytes(Integer.MAX_VALUE - i));
        }
    }

    @Test
    public void addDescending() throws IOException {
        byte[] primitiveBuffer = new byte[8];
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 96;
        int keySize = 4;
        int payloadSize = 4;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, primitiveBuffer);

        for (int i = capacity; i > 0; i--) {
            sls.add(f, from, FilerIO.intBytes(i), FilerIO.intBytes(Integer.MAX_VALUE - i), primitiveBuffer);
        }

        for (int i = capacity; i > 0; i--) {
            assertEquals(sls.getExistingPayload(f, from, FilerIO.intBytes(i), primitiveBuffer), FilerIO.intBytes(Integer.MAX_VALUE - i));
        }
    }

    @Test
    public void addRandom() throws IOException {
        byte[] primitiveBuffer = new byte[8];
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 96;
        int keySize = 4;
        int payloadSize = 4;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, primitiveBuffer);

        int[] keys = new int[capacity];
        Random rand = new Random();
        for (int i = 0; i < capacity; i++) {
            keys[i] = rand.nextInt();
        }

        for (int i = 0; i < capacity; i++) {
            sls.add(f, from, FilerIO.intBytes(keys[i]), FilerIO.intBytes(Integer.MAX_VALUE - i), primitiveBuffer);
        }

        for (int i = 0; i < capacity; i++) {
            assertEquals(sls.getExistingPayload(f, from, FilerIO.intBytes(keys[i]), primitiveBuffer), FilerIO.intBytes(Integer.MAX_VALUE - i));
        }
    }

    @Test
    public void reopenTest() throws IOException {
        byte[] primitiveBuffer = new byte[8];
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 16;
        int keySize = 1;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, primitiveBuffer);

        final HashSet<Byte> expectedContains = new HashSet<Byte>();
        for (int i = 0; i < 10; i++) {
            sls.add(f, from, new byte[]{(byte) i}, new byte[0], primitiveBuffer);
            expectedContains.add((byte) i);
        }

        assertContents(sls, f, from, new HashSet<>(expectedContains), primitiveBuffer);

        SkipListMapContext reopen = sls.open(headKey, LexSkipListComparator.cSingleton, f, primitiveBuffer);
        assertContents(sls, f, reopen, new HashSet<>(expectedContains), primitiveBuffer);

    }

    @Test
    public void getsTest() throws IOException {
        byte[] primitiveBuffer = new byte[8];
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 16;
        int keySize = 1;
        int payloadSize = 1;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, primitiveBuffer);

        for (int i = 0; i < 10; i++) {
            sls.add(f, from, new byte[]{(byte) i}, new byte[]{(byte) i}, primitiveBuffer);
        }

        assertEquals(10, sls.getCount(f, from, primitiveBuffer));

        assertEquals(new byte[]{0}, sls.getFirst(f, from, primitiveBuffer));
        assertEquals(new byte[]{1}, sls.getNextKey(f, from, new byte[]{0}, primitiveBuffer));
        assertEquals(new byte[]{0}, sls.getPrior(f, from, new byte[]{1}, primitiveBuffer));

        assertEquals(null, sls.getPrior(f, from, new byte[]{0}, primitiveBuffer));
        assertEquals(null, sls.getNextKey(f, from, new byte[]{9}, primitiveBuffer));

        assertEquals(new byte[]{9}, sls.getExistingPayload(f, from, new byte[]{9}, primitiveBuffer));
        assertEquals(null, sls.getExistingPayload(f, from, new byte[]{11}, primitiveBuffer));

    }

    @Test
    public void removeTest() throws IOException {
        byte[] primitiveBuffer = new byte[8];
        ByteBufferFactory provider = new HeapByteBufferFactory();
        int capacity = 16;
        int keySize = 1;
        int payloadSize = 0;
        SkipListMapStore sls = SkipListMapStore.INSTANCE;
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler f = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, primitiveBuffer);

        final HashSet<Byte> expectedContains = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            sls.add(f, from, new byte[]{(byte) i}, new byte[0], primitiveBuffer);
            expectedContains.add((byte) i);
        }

        assertContents(sls, f, from, new HashSet<>(expectedContains), primitiveBuffer);

        // remove from middle
        sls.remove(f, from, new byte[]{5}, primitiveBuffer);
        expectedContains.remove((byte) 5);

        assertContents(sls, f, from, new HashSet<>(expectedContains), primitiveBuffer);

        // remove first
        sls.remove(f, from, new byte[]{0}, primitiveBuffer);
        expectedContains.remove((byte) 0);

        assertContents(sls, f, from, new HashSet<>(expectedContains), primitiveBuffer);

        // remove last
        sls.remove(f, from, new byte[]{9}, primitiveBuffer);
        expectedContains.remove((byte) 9);

        assertContents(sls, f, from, new HashSet<>(expectedContains), primitiveBuffer);

        // remove second from last
        sls.remove(f, from, new byte[]{7}, primitiveBuffer);
        expectedContains.remove((byte) 7);

        assertContents(sls, f, from, new HashSet<>(expectedContains), primitiveBuffer);

        // remove second from first
        sls.remove(f, from, new byte[]{2}, primitiveBuffer);
        expectedContains.remove((byte) 2);

        assertContents(sls, f, from, new HashSet<>(expectedContains), primitiveBuffer);

    }

    private void assertContents(SkipListMapStore sls, ByteBufferBackedFiler f, SkipListMapContext from, final HashSet<Byte> expectedContains,
        byte[] primitiveBuffer)
        throws IOException {
        sls.streamKeys(f, from, new Object(), null, key -> {
            if (key != null) {
                Assert.assertTrue(expectedContains.contains(key[0]), "Expected:" + key[0]);
                expectedContains.remove(key[0]);
            }
            return true;
        }, primitiveBuffer);
        Assert.assertTrue(expectedContains.isEmpty());
    }

    @Test
    public void copyToTest() throws IOException {
        byte[] primitiveBuffer = new byte[8];
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
        SkipListMapContext from = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, f, primitiveBuffer);
        final HashSet<Byte> expectedContains = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            byte[] key = new byte[]{(byte) random.nextInt(64)};
            System.out.println("add:" + Arrays.toString(key));
            sls.add(f, from, key, new byte[0], primitiveBuffer);
            expectedContains.add(key[0]);
        }

        capacity = 64;
        slsFilerSize = sls.computeFilerSize(capacity, keySize, true, payloadSize, (byte) 9);
        ByteBufferBackedFiler t = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        SkipListMapContext to = sls.create(capacity, headKey, keySize, true, payloadSize, (byte) 9, LexSkipListComparator.cSingleton, t, primitiveBuffer);
        sls.copyTo(f, from, t, to, (fromIndex, toIndex) -> System.out.println(fromIndex + " " + toIndex), primitiveBuffer);

        sls.toSysOut(t, to, new SkipListMapStore.BytesToBytesString(), primitiveBuffer);
        assertContents(sls, t, to, expectedContains, primitiveBuffer);
    }

    @Test
    public void variableKeysTest() throws IOException {
        byte[] primitiveBuffer = new byte[8];
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
            primitiveBuffer);

        for (int i = 0; i < 2; i++) {
            byte[] key = new byte[1 + random.nextInt(keySize - 1)];
            random.nextBytes(key);
            System.out.println("add:" + Arrays.toString(key));
            sls.add(filer, context, key, new byte[0], primitiveBuffer);
        }

        sls.toSysOut(filer, context, new SkipListMapStore.BytesToBytesString() {
            @Override
            public String bytesToString(byte[] bytes) {
                return Arrays.toString(bytes);
            }
        }, primitiveBuffer);

        sls.streamKeys(filer, context, new Object(), null, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            return true;
        }, primitiveBuffer);

    }

    @Test
    public void streamRangesTest() throws IOException {
        byte[] primitiveBuffer = new byte[8];
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
            primitiveBuffer);

        for (int i = 0; i < 200; i++) {
            sls.add(filer, context, FilerIO.intBytes(i), new byte[0], primitiveBuffer);
        }

        List<KeyRange> ranges = new ArrayList<>();
        ranges.add(new KeyRange(FilerIO.intBytes(10), FilerIO.intBytes(21)));
        final AtomicInteger count = new AtomicInteger();
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, primitiveBuffer);
        assertEquals(count.intValue(), 11);

    }

    @Test
    public void prefixStreamTest() throws IOException {
        byte[] primitiveBuffer = new byte[8];
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
            primitiveBuffer);

        Random random = new Random(1234);
        for (int i = 0; i < 10; i++) {
            byte[] key = new byte[2];
            random.nextBytes(key);
            System.out.println("add:" + Arrays.toString(key));
            sls.add(filer, context, key, new byte[0], primitiveBuffer);
        }

        sls.toSysOut(filer, context, new SkipListMapStore.BytesToBytesString(), primitiveBuffer);

        final AtomicInteger count = new AtomicInteger();

        List<KeyRange> ranges = new ArrayList<>();
        ranges.add(new KeyRange(new byte[]{19, -5}, new byte[]{50}));
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, primitiveBuffer);
        assertEquals(count.intValue(), 1);

        System.out.println("-------");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{19}, new byte[]{50, -126}));
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, primitiveBuffer);
        assertEquals(count.intValue(), 3);

        System.out.println("-------");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{-11}, new byte[]{-4}));
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, primitiveBuffer);
        assertEquals(count.intValue(), 2);

        System.out.println("-------");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{-11}, new byte[]{-5}));
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, primitiveBuffer);
        assertEquals(count.intValue(), 1);

        System.out.println("------- -2, -1");
        count.set(0);
        ranges.clear();
        ranges.add(new KeyRange(new byte[]{-2}, new byte[]{-1}));
        sls.streamKeys(filer, context, new Object(), ranges, key -> {
            System.out.println("stream:" + Arrays.toString(key));
            count.incrementAndGet();
            return true;
        }, primitiveBuffer);
        assertEquals(count.intValue(), 0);

    }

    @Test
    public void slsSortRandDoublesTest() throws IOException, Exception {
        byte[] primitiveBuffer = new byte[8];
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
                primitiveBuffer);
            for (int i = 0; i < insert; i++) {
                byte[] doubleBytes = FilerIO.doubleBytes(random.nextInt(range));
                //System.out.println(i + " Added:" + FilerIO.bytesDouble(doubleBytes));
                sls.add(filer, page, doubleBytes, new byte[0], primitiveBuffer);
            }

            System.out.println("\n Count:" + MapStore.INSTANCE.getCount(filer, primitiveBuffer));

            System.out.println("\nPrint sls:");
            SkipListMapStore.BytesToString toStringer = new SkipListMapStore.BytesToDoubleString();
            sls.toSysOut(filer, page, toStringer, primitiveBuffer);

            byte[] find = FilerIO.doubleBytes(random.nextInt(range));
            byte[] got = sls.findWouldInsertAtOrAfter(filer, page, find, primitiveBuffer);
            System.out.println("\n" + FilerIO.byteDouble(find) + " would be inserted at or after " + FilerIO.byteDouble(got));

        }

    }
}
