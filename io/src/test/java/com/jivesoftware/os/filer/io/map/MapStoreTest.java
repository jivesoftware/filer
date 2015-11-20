package com.jivesoftware.os.filer.io.map;

import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MapStoreTest {

    @Test
    public void addRemove() throws IOException {
         byte[] primitiveBuffer = new byte[8];

        int filerSize = MapStore.INSTANCE.computeFilerSize(4, 1, false, 1, false);
        ByteBufferBackedFiler filer = new ByteBufferBackedFiler(ByteBuffer.allocate(filerSize));
        MapContext context = MapStore.INSTANCE.create(4, 1, false, 1, false, filer,primitiveBuffer);
        int s = 0;
        int c = 4;
        for (int i = s; i < c; i++) {
            byte[] k = new byte[]{(byte) i};
            long ai = MapStore.INSTANCE.get(filer, context, k,primitiveBuffer);
            Assert.assertTrue(ai == -1);
            MapStore.INSTANCE.add(filer, context, (byte) 1, k, k,primitiveBuffer);
            ai = MapStore.INSTANCE.get(filer, context, k,primitiveBuffer);
            Assert.assertTrue(ai != -1);
        }
        System.out.println("Before");
        MapStore.INSTANCE.toSysOut(filer, context,primitiveBuffer);

        int filerSize2 = MapStore.INSTANCE.computeFilerSize(4, 1, false, 1, false);
        Filer filer2 = new ByteBufferBackedFiler(ByteBuffer.allocate(filerSize2));
        MapContext context2 = MapStore.INSTANCE.create(4, 1, false, 1, false, filer2,primitiveBuffer);

        MapStore.INSTANCE.copyTo(filer, context, filer2, context2, null,primitiveBuffer);

        System.out.println("After:");
        MapStore.INSTANCE.toSysOut(filer2, context2,primitiveBuffer);

        for (int i = s; i < c; i++) {
            byte[] k = new byte[]{(byte) i};
            long ai = MapStore.INSTANCE.get(filer2, context2, k,primitiveBuffer);
            Assert.assertTrue(ai != -1);
        }

        for (int i = s; i < c - 2; i++) {
            byte[] k = new byte[]{(byte) i};
            MapStore.INSTANCE.remove(filer2, context2, k,primitiveBuffer);
            long ai = MapStore.INSTANCE.get(filer2, context2, k,primitiveBuffer);
            Assert.assertTrue(ai == -1);
        }

        System.out.println("AfterRemove:");
        MapStore.INSTANCE.toSysOut(filer2, context2,primitiveBuffer);

        final Set<Long> ids = new HashSet<>();
        MapStore.INSTANCE.get(filer2, context2, i -> {
            if (i > -1) {
                ids.add(i);
            }
            return true;
        });
        Assert.assertEquals(ids.size(), 2);
    }

    @Test
    public void copy() throws IOException {
          byte[] primitiveBuffer = new byte[8];
       int filerSize = MapStore.INSTANCE.computeFilerSize(2, 1, false, 1, false);
        Filer filer = new ByteBufferBackedFiler(ByteBuffer.allocate(filerSize));
        MapContext context = MapStore.INSTANCE.create(2, 1, false, 1, false, filer,primitiveBuffer);

        for (int i = 0; i < 128; i++) {
            if (MapStore.INSTANCE.isFull(context)) {
                int nextSize = MapStore.INSTANCE.nextGrowSize(context);
                System.out.println("nextSize:" + nextSize);
                filerSize = MapStore.INSTANCE.computeFilerSize(nextSize, 1, false, 1, false);
                Filer newFiler = new ByteBufferBackedFiler(ByteBuffer.allocate(filerSize));
                MapContext newContext = MapStore.INSTANCE.create(nextSize, 1, false, 1, false, newFiler,primitiveBuffer);

                MapStore.INSTANCE.copyTo(filer, context, newFiler, newContext, null,primitiveBuffer);
                filer = newFiler;
                context = newContext;

                for (int j = 0; j < i; j++) {
                    byte[] key = new byte[]{(byte) j};
                    byte[] got = MapStore.INSTANCE.getPayload(newFiler, newContext, key,primitiveBuffer);
                    System.out.println("Expected:" + Arrays.toString(key) + " Got:" + Arrays.toString(got));
                    Assert.assertEquals(got, new byte[]{(byte) j});
                }

            }
            MapStore.INSTANCE.add(filer, context, (byte) 1, new byte[]{(byte) i}, new byte[]{(byte) i},primitiveBuffer);
        }

        Object lock = new Object();
        final Set<Integer> keys = new HashSet<>();
        MapStore.INSTANCE.streamKeys(filer, context, lock, key -> {
            keys.add((int) key[0]);
            return true;
        },primitiveBuffer);
        Assert.assertEquals(keys.size(), 128);

        MapStore.INSTANCE.stream(filer, context, lock, entry -> {
            Assert.assertEquals(entry.key, entry.payload);
            return true;
        },primitiveBuffer);

        MapStore.INSTANCE.toSysOut(filer, context,primitiveBuffer);

        MapContext reopened = MapStore.INSTANCE.open(filer,primitiveBuffer);

        MapStore.INSTANCE.stream(filer, reopened, lock, entry -> {
            Assert.assertEquals(entry.key, entry.payload, new String(entry.key) + " " + new String(entry.payload));
            return true;
        },primitiveBuffer);

        for (int i = 0; i < 64; i++) {
            MapStore.INSTANCE.remove(filer, context, new byte[]{(byte) i},primitiveBuffer);
        }

        final Set<Integer> keysAfterRemove = new HashSet<>();
        MapStore.INSTANCE.streamKeys(filer, reopened, lock, key -> {
            keysAfterRemove.add((int) key[0]);
            return true;
        },primitiveBuffer);
        Assert.assertEquals(keysAfterRemove.size(), 64);

    }

    @Test(enabled = false)
    public void basicTest() throws IOException {
        test();
    }

    /**
     * @param _args
     */
    public static void main(final String[] _args) {
        for (int i = 0; i < 1; i++) {
            final int n = i;
            Thread t = new Thread() {

                @Override
                public void run() {
                    try {
                        test();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
            };
            t.start();
        }
    }

    public static void test() throws IOException {

        int it = 1_000_000;
        int ksize = 4;
        for (int i = 0; i < 10; i++) {
            System.out.println("----------------- " + i + " -----------------");
            test(it, ksize, it, new HeapByteBufferFactory());
        }
    }

    private static Boolean test(final int _iterations,
        final int keySize,
        final int _maxSize,
        HeapByteBufferFactory provider)
        throws IOException {

          byte[] primitiveBuffer = new byte[8];
       final MapStore mapStore = MapStore.INSTANCE;
        final int payloadSize = 4;

        System.out.println("Upper Bound Max Count = " + mapStore.absoluteMaxCount(keySize, payloadSize));
        int filerSize = mapStore.computeFilerSize(_maxSize, keySize, false, payloadSize, false);
        Filer filer = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), filerSize));

        MapContext set = mapStore.create(_maxSize, keySize, false, payloadSize, false, filer,primitiveBuffer);
        long seed = System.currentTimeMillis();
        int maxCapacity = mapStore.getCapacity(filer,primitiveBuffer);

        Random random = new Random(seed);
        long t = System.currentTimeMillis();
        for (int i = 0; i < _iterations; i++) {
            try {
                mapStore.add(filer, set, (byte) 1, TestUtils.randomLowerCaseAlphaBytes(random, keySize), FilerIO.intBytes(i),primitiveBuffer);
            } catch (OverCapacityException x) {
                break;
            }
        }
        long elapse = System.currentTimeMillis() - t;

        random = new Random(seed);
        t = System.currentTimeMillis();
        HashSet<String> jset = new HashSet<>(maxCapacity);
        for (int i = 0; i < _iterations; i++) {
            jset.add(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
        }
        elapse = System.currentTimeMillis() - t;
        System.out.println("JavaHashSet add(" + _iterations + ") took " + elapse);

        random = new Random(seed);
        for (int i = 0; i < set.count; i++) {
            byte[] got = mapStore.getPayload(filer, set, TestUtils.randomLowerCaseAlphaBytes(random, keySize),primitiveBuffer);
            assert got != null : "shouldn't be null";
            //int v = UIO.bytesInt(got);
            //assert v == i : "should be the same";
        }

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _iterations; i++) {
            try {
                mapStore.remove(filer, set, TestUtils.randomLowerCaseAlphaBytes(random, keySize),primitiveBuffer);
            } catch (Exception x) {
                x.printStackTrace();
                break;
            }
        }
        elapse = System.currentTimeMillis() - t;
        System.out.println("ByteSet remove(" + _iterations + ") took " + elapse + " " + set.count);

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _iterations; i++) {
            try {
                jset.remove(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
            } catch (Exception x) {
                x.printStackTrace();
                break;
            }
        }
        elapse = System.currentTimeMillis() - t;
        System.out.println("JavaHashSet remove(" + _iterations + ") took " + elapse);

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _maxSize; i++) {
            if (i % 2 == 0) {
                mapStore.remove(filer, set, TestUtils.randomLowerCaseAlphaBytes(random, keySize),primitiveBuffer);
            } else {
                mapStore.add(filer, set, (byte) 1, TestUtils.randomLowerCaseAlphaBytes(random, keySize), FilerIO.intBytes(i),primitiveBuffer);
            }
        }
        elapse = System.currentTimeMillis() - t;
        System.out.println("ByteSet add and remove (" + _maxSize + ") took " + elapse + " " + set.count);

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _maxSize; i++) {
            if (i % 2 == 0) {
                jset.remove(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
            } else {
                jset.add(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
            }
        }
        elapse = System.currentTimeMillis() - t;
        System.out.println("JavaHashSet add and remove (" + _maxSize + ") took " + elapse);

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _maxSize; i++) {
            mapStore.contains(filer, set, TestUtils.randomLowerCaseAlphaBytes(random, keySize),primitiveBuffer);
        }
        elapse = System.currentTimeMillis() - t;
        System.out.println("ByteSet contains (" + _maxSize + ") took " + elapse + " " + set.count);

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _maxSize; i++) {
            jset.contains(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
        }
        elapse = System.currentTimeMillis() - t;
        System.out.println("JavaHashSet contains (" + _maxSize + ") took " + elapse);

        random = new Random(seed);
        for (int i = 0; i < _maxSize; i++) {
            if (i % 2 == 0) {
                TestUtils.randomLowerCaseAlphaBytes(random, keySize);
            } else {
                mapStore.getPayload(filer, set, TestUtils.randomLowerCaseAlphaBytes(random, keySize),primitiveBuffer);
                //assert got == i;
            }
        }
        System.out.println("count " + set.count);

        return true;

    }

}
