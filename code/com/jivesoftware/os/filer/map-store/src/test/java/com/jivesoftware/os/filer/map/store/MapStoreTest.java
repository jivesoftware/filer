package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.ByteBufferBackedConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.MonkeyFilerTransaction;
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class MapStoreTest {

    @Test
    public void copy() throws IOException {
        int filerSize = MapStore.INSTANCE.computeFilerSize(2, 1, false, 1, false);
        Filer filer = new ByteBufferBackedFiler(this, ByteBuffer.allocate(filerSize));
        MapContext context = MapStore.INSTANCE.create(2, 1, false, 1, false, filer);

        for (int i = 0; i < 128; i++) {
            if (MapStore.INSTANCE.isFull(filer, context)) {
                int nextSize = MapStore.INSTANCE.nextGrowSize(context);
                System.out.println("nextSize:" + nextSize);
                filerSize = MapStore.INSTANCE.computeFilerSize(nextSize, 1, false, 1, false);
                Filer newFiler = new ByteBufferBackedFiler(this, ByteBuffer.allocate(filerSize));
                MapContext newContext = MapStore.INSTANCE.create(nextSize, 1, false, 1, false, newFiler);

                MapStore.INSTANCE.copyTo(filer, context, newFiler, newContext, null);
                filer = newFiler;
                context = newContext;

                for (int j = 0; j < i; j++) {
                    byte[] key = new byte[]{(byte) j};
                    byte[] got = MapStore.INSTANCE.getPayload(newFiler, newContext, key);
                    System.out.println("Expected:" + Arrays.toString(key) + " Got:" + Arrays.toString(got));
                    Assert.assertEquals(got, new byte[]{(byte) j});
                }

            }
            MapStore.INSTANCE.add(filer, context, (byte) 1, new byte[]{(byte) i}, new byte[]{(byte) i});

        }
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
            test(it, ksize, it, new ConcurrentFilerProvider<>("booya".getBytes(Charsets.UTF_8),
                new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory())));
        }
    }

    private static Boolean test(final int _iterations,
        final int keySize,
        final int _maxSize,
        ConcurrentFilerProvider<ByteBufferBackedFiler> provider)
        throws IOException {

        final MapStore mapStore = MapStore.INSTANCE;
        final int payloadSize = 4;

        System.out.println("Upper Bound Max Count = " + mapStore.absoluteMaxCount(keySize, payloadSize));
        int filerSize = mapStore.computeFilerSize(_maxSize, keySize, false, payloadSize, false);
        return provider.getOrAllocate(filerSize, new NoOpOpenFiler<ByteBufferBackedFiler>(), new NoOpCreateFiler<ByteBufferBackedFiler>(),
            new MonkeyFilerTransaction<Void, ByteBufferBackedFiler, Boolean>() {
                @Override
                public Boolean commit(Void monkey, ByteBufferBackedFiler filer) throws IOException {
                    MapContext set = mapStore.create(_maxSize, keySize, false, payloadSize, false, filer);
                    long seed = System.currentTimeMillis();
                    int maxCapacity = mapStore.getCapacity(filer);

                    Random random = new Random(seed);
                    long t = System.currentTimeMillis();
                    for (int i = 0; i < _iterations; i++) {
                        try {
                            mapStore.add(filer, set, (byte) 1, TestUtils.randomLowerCaseAlphaBytes(random, keySize), FilerIO.intBytes(i));
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
                        byte[] got = mapStore.getPayload(filer, set, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
                        assert got != null : "shouldn't be null";
                        //int v = UIO.bytesInt(got);
                        //assert v == i : "should be the same";
                    }

                    random = new Random(seed);
                    t = System.currentTimeMillis();
                    for (int i = 0; i < _iterations; i++) {
                        try {
                            mapStore.remove(filer, set, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
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
                            mapStore.remove(filer, set, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
                        } else {
                            mapStore.add(filer, set, (byte) 1, TestUtils.randomLowerCaseAlphaBytes(random, keySize), FilerIO.intBytes(i));
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
                        mapStore.contains(filer, set, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
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
                            mapStore.getPayload(filer, set, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
                            //assert got == i;
                        }
                    }
                    System.out.println("count " + set.count);

                    return true;
                }
            });
    }

}
