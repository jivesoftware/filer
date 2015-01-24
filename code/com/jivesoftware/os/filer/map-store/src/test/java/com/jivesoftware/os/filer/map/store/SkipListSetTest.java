package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

/**
 * @author jonathan
 */
public class SkipListSetTest {

    @Test
    public void slsSortRandDoublesTest() throws IOException, Exception {

        int capacity = 128;
        int keySize = 8;
        int payloadSize = 0;
        int range = 63;
        int insert = 64;
        ByteBufferFactory provider = new HeapByteBufferFactory();

        SkipListSet sls = new SkipListSet();
        long slsFilerSize = sls.computeFilerSize(capacity, keySize, payloadSize);
        ByteBufferBackedFiler filer = new ByteBufferBackedFiler(provider.allocate("booya".getBytes(), slsFilerSize));

        Random random = new Random(1234);
        byte[] headKey = new byte[keySize];
        Arrays.fill(headKey, Byte.MIN_VALUE);
        SkipListSetContext page = sls.create(capacity, headKey, keySize, payloadSize, DoubleSkipListComparator.cSingleton, filer);
        for (int i = 0; i < insert; i++) {
            byte[] doubleBytes = FilerIO.doubleBytes(random.nextInt(range));
            System.out.println(i+" Added:" + FilerIO.bytesDouble(doubleBytes));
            sls.sladd(filer, page, doubleBytes, new byte[0]);
        }

        System.out.println("\n Count:" + sls.map.getCount(filer));

        System.out.println("\nPrint sls:");
        SkipListSet.BytesToString toStringer = new SkipListSet.BytesToDoubleString();
        sls.sltoSysOut(filer, page, toStringer);

        byte[] find = FilerIO.doubleBytes(random.nextInt(range));
        byte[] got = sls.slfindWouldInsertAfter(filer, page, find);
        System.out.println("\n" + FilerIO.byteDouble(find) + " would be inserted at or after " + FilerIO.byteDouble(got));

        final AtomicInteger i = new AtomicInteger(1);
        System.out.println("\nRange from " + FilerIO.byteDouble(find) + " max after 4");
        sls.slgetSlice(filer, page, find, null, 4, new SkipListSet.ExtractorStream<KeyPayload, Exception>() {

            @Override
            public KeyPayload stream(KeyPayload v) throws Exception {
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
        sls.slgetSlice(filer, page, FilerIO.doubleBytes(from), FilerIO.doubleBytes(from + to), -1, new SkipListSet.ExtractorStream<KeyPayload, Exception>() {

            @Override
            public KeyPayload stream(KeyPayload v) throws Exception {
                if (v != null) {
                    System.out.println(i + ":" + FilerIO.byteDouble(v.key)); //toStringer.bytesToString(v.key));
                    i.incrementAndGet();
                }
                return v;
            }
        });

    }
}
