package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.ByteBufferBackedConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.map.store.extractors.IndexStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author jonathan
 */
public class SkipListSet_Test {

    /**
     * @param _args
     */
    public static void main(String[] _args) throws IOException {

        ConcurrentFilerProvider<ByteBufferBackedFiler> provider = new ConcurrentFilerProvider<>("booya".getBytes(Charsets.UTF_8),
            new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory()));

        //chart(factory);
        //System.exit(0);
        int it = 30;
        test(it, 2, it, provider);
        System.exit(0);

        long seed = System.currentTimeMillis();
        int keySize = 1;
        int payloadSize = 2;
        it = 150;

        SkipListSet sls = new SkipListSet();
        byte[] headKey = new byte[]{Byte.MIN_VALUE};

        MapStore pset = MapStore.INSTANCE;

        SkipListSetPage slsp = sls.slallocate(pset, it, headKey, keySize, false, payloadSize, false, new SkipListComparator() {

            @Override
            public int compare(MapContext a, int astart, MapContext b, int bstart, int length) throws IOException {
                for (int i = 0; i < length; i++) {
                    byte av = a.read(astart + i);
                    byte bv = a.read(bstart + i);

                    if (av == bv) {
                        continue;
                    }
                    if (av < bv) {
                        return -1;
                    }
                    for (int j = i; j < length; j++) {
                        if (av < bv) {
                            return -1;
                        }
                    }
                    return 1;
                }
                return 0;
            }

            @Override
            public long range(byte[] a, byte[] b) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }, provider);

        Random random = new Random(1_234);
        byte[] a = new byte[]{65}; //URandom.randomLowerCaseAlphaBytes(keySize);
        byte[] b = new byte[]{66}; //URandom.randomLowerCaseAlphaBytes(keySize);
        byte[] c = new byte[]{67}; //URandom.randomLowerCaseAlphaBytes(keySize);
        byte[] k4 = new byte[]{68}; //URandom.randomLowerCaseAlphaBytes(keySize);
        byte[] payload1 = TestUtils.randomLowerCaseAlphaBytes(random, payloadSize);

        int n = 1;
        n = p(n, c, sls, slsp, payload1);
        n = p(n, a, sls, slsp, payload1);
        n = p(n, b, sls, slsp, payload1);

        //System.exit(0);
        //for(int i=0;i<1000;i++) {
        n = p(n, c, sls, slsp, payload1);
        n = p(n, a, sls, slsp, payload1);
        //n = m(n,b,sls);
        n = p(n, b, sls, slsp, payload1);
        n = m(n, b, sls, slsp);
        n = p(n, a, sls, slsp, payload1);
        n = p(n, b, sls, slsp, payload1);

        //}
        //System.exit(0);
        Object[] keys = new Object[]{a, b, c};
        for (int i = 0; i < 100_000; i++) {
            if (Math.random() < 0.5d) {
                byte[] ak = (byte[]) keys[random.nextInt(keys.length)];
                System.out.println("+" + new String(ak) + " " + sls.slgetCount(slsp));
                sls.sladd(slsp, ak, payload1);
            } else {
                byte[] rk = (byte[]) keys[random.nextInt(keys.length)];
                System.out.println("-" + new String(rk) + " " + sls.slgetCount(slsp));
                sls.slremove(slsp, rk);
            }
        }

        //System.exit(0);
        random = new Random(seed);
        for (int i = 0; i < it; i++) {
            byte[] key = TestUtils.randomLowerCaseAlphaBytes(random, keySize);
            byte[] payload = TestUtils.randomLowerCaseAlphaBytes(random, payloadSize);
            //System.out.println("adding:"+new String(key));
            sls.sladd(slsp, key, payload);
        }
        sls.sltoSysOut(slsp, null);
        System.out.println("count:" + sls.slgetCount(slsp));

        random = new Random(seed);
        for (int i = 0; i < it; i++) {
            byte[] key = TestUtils.randomLowerCaseAlphaBytes(random, keySize);
            byte[] payload = TestUtils.randomLowerCaseAlphaBytes(random, payloadSize); // burns through random at the same rate as add
            //System.out.println("removing:"+new String(key));
            sls.slremove(slsp, key);
            //sls.toSysOut();
        }
        //sls.toSysOut();
        if (sls.slgetCount(slsp) != 0) {
            sls.mapStore.get(slsp.chunk, new IndexStream<Exception>() {

                @Override
                public boolean stream(long v) throws Exception {
                    if (v != -1) {
                        System.out.println(v);
                    }
                    return true;
                }
            });
        }
        //sls.toSysOut();
        System.out.println("count:" + sls.slgetCount(slsp));
        random = new Random(seed);
        for (int i = 0; i < it; i++) {
            byte[] key = TestUtils.randomLowerCaseAlphaBytes(random, keySize);
            byte[] payload = TestUtils.randomLowerCaseAlphaBytes(random, payloadSize); // burns through random at the same rate as add
            //System.out.println("removing:"+new String(key));
            if (i % 2 == 0) {
                //System.out.println("-"+sls.getCount());
                sls.slremove(slsp, key);
            } else {
                //System.out.println("+"+sls.getCount());
                sls.sladd(slsp, key, payload);
            }
            //sls.toSysOut();
        }
        System.out.println("count:" + sls.slgetCount(slsp));

    }

    private static int p(int n, byte[] v, SkipListSet sls, SkipListSetPage slsp, byte[] p) throws IOException {
        System.out.println(n + ": add " + new String(v) + " " + sls.slgetCount(slsp));
        sls.sladd(slsp, v, p);
        sls.sltoSysOut(slsp, null);
        return n + 1;
    }

    private static int m(int n, byte[] v, SkipListSet sls, SkipListSetPage slsp) throws IOException {
        System.out.println(n + ": remove " + new String(v) + " " + sls.slgetCount(slsp));
        sls.slremove(slsp, v);
        sls.sltoSysOut(slsp, null);
        return n + 1;
    }

    private static boolean test(int _iterations, int _keySize, int _maxSize, ConcurrentFilerProvider provider) throws IOException {
        byte[] headKey = new byte[_keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        int keySize = _keySize;
        int payloadSize = 4;
        SkipListSet sls = new SkipListSet();
        MapStore pset = MapStore.INSTANCE;
        SkipListSetPage slsp = sls.slallocate(pset, _maxSize, headKey, keySize, false, payloadSize, false, new SkipListComparator() {

            @Override
            public int compare(MapContext a, int astart, MapContext b, int bstart, int length) throws IOException {
                for (int i = 0; i < length; i++) {
                    byte av = a.read(astart + i);
                    byte bv = b.read(bstart + i);
                    if (av == bv) {
                        continue;
                    }
                    if (av < bv) {
                        return -1;
                    }
                    for (int j = i; j < length; j++) {
                        if (av < bv) {
                            return -1;
                        }
                    }
                    return 1;
                }
                return 0;
            }

            @Override
            public long range(byte[] a, byte[] b) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }, provider);

        System.out.println("MaxCount = " + sls.mapStore.getMaxCount(slsp.chunk) + " vs " + _iterations + " vs " + sls.mapStore.getCapacity(slsp.chunk));
        System.out.println("Upper Bound Max Count = " + sls.mapStore.absoluteMaxCount(sls.mapStore.getKeySize(slsp.chunk), sls.mapStore.getPayloadSize(slsp.chunk)));
        long seed = System.currentTimeMillis();

        System.out.println("\nadd:");
        Random random = new Random(seed);
        long t = System.currentTimeMillis();
        for (int i = 0; i < _iterations; i++) {
            sls.sladd(slsp, TestUtils.randomLowerCaseAlphaBytes(random, keySize), FilerIO.intBytes(i));
        }
        System.out.println("ByteSL add(" + _iterations + ") took " + (System.currentTimeMillis() - t) + " Size:" + sls.slgetCount(slsp));

        random = new Random(seed);
        t = System.currentTimeMillis();
        ConcurrentSkipListSet jsl = new ConcurrentSkipListSet(new Comparator() {

            @Override
            public int compare(Object o1, Object o2) {
                return ((Comparable<String>) o1).compareTo((String) o2);
            }
        });
        for (int i = 0; i < _iterations; i++) {
            jsl.add(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
        }
        System.out.println("Java ConcurrentSkipListSet add(" + _iterations + ") took " + (System.currentTimeMillis() - t) + " Size:" + jsl.size());

        random = new Random(seed);
        t = System.currentTimeMillis();
        TreeSet jtree = new TreeSet(new Comparator() {

            @Override
            public int compare(Object o1, Object o2) {
                return ((Comparable<String>) o1).compareTo((String) o2);
            }
        });
        for (int i = 0; i < _iterations; i++) {
            jtree.add(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
        }
        System.out.println("Java TreeSet add(" + _iterations + ") took " + (System.currentTimeMillis() - t) + " Size:" + jtree.size());

        System.out.println("\nremove:");
        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _iterations; i++) {
            sls.slremove(slsp, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
        }
        System.out.println("ByteSL remove(" + _iterations + ") took " + (System.currentTimeMillis() - t) + " Size:" + sls.slgetCount(slsp));

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _iterations; i++) {
            jsl.remove(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
        }
        System.out.println("Java ConcurrentSkipListSet remove(" + _iterations + ") took " + (System.currentTimeMillis() - t) + " Size:" + jsl.size());

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _iterations; i++) {
            jtree.remove(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
        }
        System.out.println("Java TreeSet remove(" + _iterations + ") took " + (System.currentTimeMillis() - t) + " Size:" + jtree.size());

        System.out.println("\nadd and remove:");
        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _maxSize; i++) {
            if (i % 2 == 0) {
                sls.slremove(slsp, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
            } else {
                sls.sladd(slsp, TestUtils.randomLowerCaseAlphaBytes(random, keySize), FilerIO.intBytes(i));
            }
        }
        System.out.println("ByteSL add and remove (" + _maxSize + ") took " + (System.currentTimeMillis() - t) + " Size:" + sls.slgetCount(slsp));

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _iterations; i++) {
            if (i % 2 == 0) {
                jsl.remove(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
            } else {
                jsl.add(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
            }
        }
        System.out.println("Java ConcurrentSkipListSet add and remove(" + _iterations + ") took " + (System.currentTimeMillis() - t) + " Size:" + jsl.size());

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _maxSize; i++) {
            if (i % 2 == 0) {
                jtree.remove(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
            } else {
                jtree.add(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
            }
        }
        System.out.println("Java TreeSet  add and remove (" + _maxSize + ") took " + (System.currentTimeMillis() - t) + " Size:" + jtree.size());

        System.out.println("\ncontains:");
        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _maxSize; i++) {
            sls.mapStore.contains(slsp.chunk, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
        }
        System.out.println("ByteSL contains (" + _maxSize + ") took " + (System.currentTimeMillis() - t) + " Size:" + sls.slgetCount(slsp));

        random = new Random(seed);
        t = System.currentTimeMillis();
        for (int i = 0; i < _maxSize; i++) {
            jsl.contains(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
        }
        System.out.println("Java ConcurrentSkipListSet  contains (" + _maxSize + ") took " + (System.currentTimeMillis() - t) + " Size:" + jsl.size());

        System.out.println("\ncontains:");
        random = new Random(seed);
        for (int i = 0; i < _maxSize; i++) {
            jtree.contains(new String(TestUtils.randomLowerCaseAlphaBytes(random, keySize)));
        }
        System.out.println("Java TreeSet  contains (" + _maxSize + ") took " + (System.currentTimeMillis() - t) + " Size:" + jtree.size());

        sls.sltoSysOut(slsp, null);

        return true;
    }

    /**
     * @param provider
     */
    public static void chart(ConcurrentFilerProvider provider) throws IOException {
        int ksize = 16;
        int payloadSize = 4;
        int maxSize = 1_000_000;

        int step = 10_000;
        System.out.println("mode,iterations,duration,size,mb");
        SkipListSet sls = new SkipListSet();

        for (int i = step; i < maxSize; i += step) {
            SkipListSetPage set = testSet(sls, null, 112_233, i, ksize, payloadSize, i, 0, true, provider);
            stats(ksize, payloadSize, i, provider);
            System.out.println();
        }
        for (int i = step; i < maxSize; i += step) {
            SkipListSetPage set = testSet(sls, null, 112_233, i, ksize, payloadSize, i, 0, false, provider);
            testSet(sls, set, 112_233, i, ksize, payloadSize, i, 1, true, provider);
            stats(ksize, payloadSize, i, provider);
            System.out.println();
        }
        for (int i = step; i < maxSize; i += step) {
            SkipListSetPage set = testSet(sls, null, 112_233, i, ksize, payloadSize, i, 2, true, provider);
            stats(ksize, payloadSize, i, provider);
            System.out.println();
        }
        for (int i = step; i < maxSize; i += step) {
            SkipListSetPage set = testSet(sls, null, 112_233, i, ksize, payloadSize, i, 2, false, provider);
            testSet(sls, set, 112_233, i, ksize, payloadSize, i, 3, true, provider);
            stats(ksize, payloadSize, i, provider);
            System.out.println();
        }
    }

    private static void stats(int keySize, int payloadSize, int _maxSize, ConcurrentFilerProvider provider) throws IOException {
        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListSet sls = new SkipListSet();
        MapStore pset = MapStore.INSTANCE;
        SkipListSetPage slsp = sls.slallocate(pset, _maxSize, headKey, keySize, false, payloadSize, false, new SkipListComparator() {

            @Override
            public int compare(MapContext a, int astart, MapContext b, int bstart, int length) throws IOException {
                for (int i = 0; i < length; i++) {
                    byte av = a.read(astart + i);
                    byte bv = b.read(bstart + i);

                    if (av == bv) {
                        continue;
                    }
                    if (av < bv) {
                        return -1;
                    }
                    for (int j = i; j < length; j++) {
                        if (av < bv) {
                            return -1;
                        }
                    }
                    return 1;
                }
                return 0;
            }

            @Override
            public long range(byte[] a, byte[] b) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }, provider);
    }

    private static SkipListSetPage testSet(SkipListSet sls,
        SkipListSetPage set, long seed, int _iterations, int keySize, int payloadSize, int _maxSize, int mode, boolean _out, ConcurrentFilerProvider provider)
        throws IOException {

        if (set == null) {
            byte[] headKey = new byte[keySize];
            Arrays.fill(headKey, Byte.MIN_VALUE);
            MapStore pset = MapStore.INSTANCE;
            set = sls.slallocate(pset, _maxSize, headKey, keySize, false, payloadSize, false, new SkipListComparator() {

                @Override
                public int compare(MapContext a, int astart, MapContext b, int bstart, int length) throws IOException {
                    for (int i = 0; i < length; i++) {
                        byte av = a.read(astart + i);
                        byte bv = b.read(bstart + i);

                        if (av == bv) {
                            continue;
                        }
                        if (av < bv) {
                            return -1;
                        }
                        for (int j = i; j < length; j++) {
                            if (av < bv) {
                                return -1;
                            }
                        }
                        return 1;
                    }
                    return 0;
                }

                @Override
                public long range(byte[] a, byte[] b) {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            }, provider);
        }

        System.out.println("\ncontains:");
        Random random = new Random(seed);
        long t = System.currentTimeMillis();
        if (mode == 0) {
            for (int i = 0; i < _iterations; i++) {
                sls.sladd(set, TestUtils.randomLowerCaseAlphaBytes(random, keySize), TestUtils.randomLowerCaseAlphaBytes(random, payloadSize));
            }
            if (_out) {
                System.out.print("add," + _iterations + "," + (System.currentTimeMillis() - t));
            }
        }
        if (mode == 1) {
            for (int i = 0; i < _iterations; i++) {
                sls.slremove(set, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
            }
            if (_out) {
                System.out.print("remove," + _iterations + "," + (System.currentTimeMillis() - t));
            }
        }
        if (mode == 2) {
            for (int i = 0; i < _iterations; i++) {
                if (i % 2 == 0) {
                    sls.slremove(set, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
                } else {
                    sls.sladd(set, TestUtils.randomLowerCaseAlphaBytes(random, keySize), TestUtils.randomLowerCaseAlphaBytes(random, payloadSize));
                }
            }
            if (_out) {
                System.out.print("add/remove," + _iterations + "," + (System.currentTimeMillis() - t));
            }
        }
        if (mode == 3) {
            for (int i = 0; i < _iterations; i++) {
                sls.mapStore.contains(set.chunk, TestUtils.randomLowerCaseAlphaBytes(random, keySize));
            }
            if (_out) {
                System.out.print("contains," + _iterations + "," + (System.currentTimeMillis() - t));
            }
        }
        return set;
    }
    // have initial impl of a skip list backed by a byte[].. looks like ~1.12mb to store 10,000 (UIDS + a long)
    // looks like ~13.08mb to store 100,000 (UIDS + a long)
    // looks like ~146.8mb to store 1,000,000 (UIDS + a long)
    // one level deep
}
