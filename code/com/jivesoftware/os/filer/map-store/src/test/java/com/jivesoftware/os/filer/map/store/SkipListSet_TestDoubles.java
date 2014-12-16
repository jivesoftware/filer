package com.jivesoftware.os.filer.map.store;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.ByteBufferBackedConcurrentFilerFactory;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ConcurrentFilerProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.MonkeyFilerTransaction;
import com.jivesoftware.os.filer.io.NoOpCreateFiler;
import com.jivesoftware.os.filer.io.NoOpOpenFiler;
import com.jivesoftware.os.filer.map.store.extractors.IndexStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jonathan
 */
public class SkipListSet_TestDoubles {

    /**
     * @param _args
     * @throws Exception
     */
    public static void main(String[] _args) throws Exception {

        final int keySize = 8;
        final int payloadSize = 0;
        MapStore pset = MapStore.INSTANCE;
        int filerSize = pset.computeFilerSize(16, keySize, false, payloadSize, false);
        ByteBufferBackedConcurrentFilerFactory concurrentFilerFactory = new ByteBufferBackedConcurrentFilerFactory(new HeapByteBufferFactory());
        ConcurrentFilerProvider<ByteBufferBackedFiler> filerProvider = new ConcurrentFilerProvider<>("booya".getBytes(Charsets
            .UTF_8), concurrentFilerFactory);
        filerProvider.getOrAllocate(filerSize, new NoOpOpenFiler<ByteBufferBackedFiler>(), new NoOpCreateFiler<ByteBufferBackedFiler>(),
            new MonkeyFilerTransaction<Void, ByteBufferBackedFiler, Void>() {

                @Override
                public Void commit(Void monkey, ByteBufferBackedFiler filer) throws IOException {
                    SkipListSet sls = new SkipListSet();
                    Random random = new Random(1_234);
                    byte[] headKey = new byte[keySize];
                    Arrays.fill(headKey, Byte.MIN_VALUE);
                    SkipListSetPage page = sls.create(16, headKey, keySize, false, payloadSize, false, DoubleSkipListComparator.cSingleton, filer);
                    for (int i = 0; i < 16; i++) {
                        sls.sladd(filer, page, FilerIO.doubleBytes(random.nextInt(32)), new byte[0]);
                    }

                    final SkipListSet.BytesToString toStringer = new SkipListSet.BytesToDoubleString();
                    sls.sltoSysOut(filer, page, toStringer);

                    byte[] find = FilerIO.doubleBytes(random.nextInt(32));
                    byte[] got = sls.slfindWouldInsertAfter(filer, page, find);

                    System.out.println(FilerIO.byteDouble(got) + " + 4 key=" + FilerIO.byteDouble(find));

                    final AtomicInteger i = new AtomicInteger(1);
                    sls.slgetSlice(filer, page, find, null, 4, new IndexStream<IOException>() {

                        @Override
                        public boolean stream(long v) throws IOException {
                            if (v != -1) {
                                System.out.println(i + ":" + v); //toStringer.bytesToString(v.key));
                                i.incrementAndGet();
                            }
                            return true;
                        }
                    });

                    i.set(1);
                    double from = random.nextInt(32);
                    double to = random.nextInt(6);
                    System.out.println(from + "->" + (from + to));
                    sls.slgetSlice(filer, page, FilerIO.doubleBytes(from), FilerIO.doubleBytes(from + to), -1, new IndexStream<IOException>() {

                        @Override
                        public boolean stream(long v) throws IOException {
                            if (v != -1) {
                                System.out.println(i + ":" + v); //toStringer.bytesToString(v.key));
                                i.incrementAndGet();
                            }
                            return true;
                        }
                    });
                    return null;
                }
            });

        System.exit(0);
    }
}
