/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.filer.queue.store;

import com.jivesoftware.os.jive.utils.base.util.UtilThread;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 * @author jonathan
 */
public class FileQueueImplTest {

    @Test
    public void testMultiplePages() throws Exception {
        System.out.println("testMultiplePages");
        MutableLong pending = new MutableLong();
        long takableWhenOlderThanXMillis = 100;
        long takableWhenLargerThanXSize = 10;
        long pageSize = 10;
        File tmpDir = getTempDir();

        String queueName = "test" + System.currentTimeMillis();
        final FileQueueImpl write = new FileQueueImpl(tmpDir.getAbsolutePath(), queueName, takableWhenOlderThanXMillis,
            takableWhenLargerThanXSize, pageSize, Integer.MAX_VALUE, true, pending);
        final int numAdds = 100;
        for (int a = 0; a < numAdds; a++) {
            try {
                write.add(PhasedQueueConstants.ENQUEUED, a, ("test-" + a).getBytes());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        // write.close();
        System.out.println("added " + numAdds + " items");

        pending = new MutableLong();
        final FileQueueImpl read = new FileQueueImpl(tmpDir.getAbsolutePath(), queueName, takableWhenOlderThanXMillis,
            takableWhenLargerThanXSize, pageSize, Integer.MAX_VALUE, true, pending);

        final AtomicLong consumedCount = new AtomicLong();
        long start = System.currentTimeMillis();
        //int id = 0;
        while (consumedCount.get() < numAdds) {
            List<PhasedQueueEntry> batch = read.consume(PhasedQueueConstants.ENQUEUED, PhasedQueueConstants.CONSUMED,
                5, Long.MAX_VALUE);
            if (batch.isEmpty()) {
                if (System.currentTimeMillis() - start > 60000) {
                    break;
                }
                Thread.sleep(100);
            } else {
                for (PhasedQueueEntry b : batch) {
                    String data = new String(b.getEntry());
                    System.out.println(data);
                    //AssertJUnit.assertEquals(data, "test-" + id);
                    consumedCount.incrementAndGet();
                    //id++;
                    b.processed();
                }
            }
        }
        AssertJUnit.assertEquals(consumedCount.get(), numAdds);

        for (File f : tmpDir.listFiles()) {
            AssertJUnit.assertFalse(f.getName().endsWith(".corrupt"));
        }
    }

    @Test
    public void testConcurrency() throws Exception {
        System.out.println("testConcurrency");
        MutableLong pending = new MutableLong();
        long takableWhenOlderThanXMillis = 1;
        long takableWhenLargerThanXSize = 1;
        File tmpDir = getTempDir();

        final FileQueueImpl q = new FileQueueImpl(tmpDir.getAbsolutePath(), "test" + System.currentTimeMillis(),
            takableWhenOlderThanXMillis, takableWhenLargerThanXSize, 10 * 1024 * 1024, Integer.MAX_VALUE, true,
            pending);
        final int numProducers = 10;
        final int initialAdds = 20000;
        final int numAdds = 100000;
        final int delayBetweenAdds = 0;
        final AtomicLong added = new AtomicLong();

        // populate queue with initial messages
        for (int i = 0; i < initialAdds; i++) {
            q.add(PhasedQueueConstants.ENQUEUED, System.currentTimeMillis(), "test".getBytes());
            added.addAndGet(1);
        }

        // check getSize() is correct
        AssertJUnit.assertEquals("Incorrect size", initialAdds, q.getSize(PhasedQueueConstants.ENQUEUED));

        for (int i = 0; i < numProducers; i++) {
            final int t = i;
            Thread thread = new Thread("consumer " + i) {
                @Override
                public void run() {
                    for (int a = 0; a < numAdds; a++) {
                        int mod = a % numProducers;
                        if (mod != t) {
                            continue;
                        }
                        try {
                            q.add(PhasedQueueConstants.ENQUEUED, System.currentTimeMillis(), "test".getBytes());
                            added.addAndGet(1);

                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        UtilThread.sleep(delayBetweenAdds);
                    }
                }
            };
            thread.start();
            // System.out.println("started producer thread "+t);
        }

        int numConsumer = 10;
        final long sleepForNMillisOnEmptyConsume = 1;
        final List<String> consumed = new LinkedList<>();
        final CountDownLatch l = new CountDownLatch(numAdds + initialAdds);
        final AtomicLong consumedCount = new AtomicLong();
        for (int i = 0; i < numConsumer; i++) {
            Thread thread = new Thread("consumer " + i) {
                @Override
                public void run() {
                    while (true) {
                        try {
                            List<PhasedQueueEntry> batch = q.consume(PhasedQueueConstants.ENQUEUED,
                                PhasedQueueConstants.CONSUMED, 1, Long.MAX_VALUE);
                            consumedCount.addAndGet(batch.size());
                            if (batch.isEmpty()) {
                                UtilThread.sleep(sleepForNMillisOnEmptyConsume);
                            } else {
                                for (PhasedQueueEntry b : batch) {
                                    String data = new String(b.getEntry());
                                    consumed.add(data);
                                    AssertJUnit.assertEquals("Incorrect data", "test", data);
                                    b.processed();
                                    l.countDown();
                                }
                            }
                            if (consumed.size() == (numAdds + initialAdds)) {
                                return;
                            }

                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            };
            thread.start();
            // System.out.println("started consumer thread "+i);
        }

        System.out.println("waiting for latch");
        l.await();
        System.out.println("pending:" + pending.longValue() + " " + added.get() + " " + consumedCount.get());
        AssertJUnit.assertEquals(initialAdds + numAdds, consumedCount.get());
    }

    @Test
    public void testFileCorruption() throws Exception {
        MutableLong pending = new MutableLong();
        long takableWhenOlderThanXMillis = 1;
        long takableWhenLargerThanXSize = 1;
        File tmpDir = getTempDir();
        String queueName = "test";

        final FileQueueImpl writeQ = new FileQueueImpl(tmpDir.getAbsolutePath(), queueName,
            takableWhenOlderThanXMillis, takableWhenLargerThanXSize, 10 * 1024 * 1024, Integer.MAX_VALUE, true,
            pending);

        // add some messages
        for (int i = 0; i < 2; i++) {
            writeQ.add(PhasedQueueConstants.ENQUEUED, System.currentTimeMillis(), "test".getBytes());
        }
        // remove some bytes from the first message in the data part - i.e. skip the header
        File queueDir = tmpDir.listFiles()[0];
        File queueFile = queueDir.listFiles()[0];
        removeBytesFromFile(queueFile, FileQueue.headerSize, 2);

        final FileQueueImpl readQ = new FileQueueImpl(tmpDir.getAbsolutePath(), queueName, takableWhenOlderThanXMillis,
            takableWhenLargerThanXSize, 10 * 1024 * 1024, Integer.MAX_VALUE, true, pending);

        Exception ex = null;
        try {
            readQ.consume(PhasedQueueConstants.ENQUEUED, PhasedQueueConstants.CONSUMED, 1, Long.MAX_VALUE);
        } catch (Exception e) {
            ex = e;
        }

        AssertJUnit.assertNull("no expected exception should be thrown", ex);

    }

    @Test
    public void testBacklog() throws Exception {
        MutableLong pending = new MutableLong();
        long takableWhenOlderThanXMillis = 1;
        long takableWhenLargerThanXSize = 1;
        final int numberOfMessages = 1000000;
        int pageSize = 1024 * 1024 * 1024;
        String queueName = "test";
        File tmpDir = getTempDir();

        final FileQueueImpl writeQ = new FileQueueImpl(tmpDir.getAbsolutePath(), queueName,
            takableWhenOlderThanXMillis, takableWhenLargerThanXSize, pageSize, Integer.MAX_VALUE, true, pending);

        // add a lot of messages
        for (int i = 0; i < numberOfMessages; i++) {
            writeQ.add(PhasedQueueConstants.ENQUEUED, System.currentTimeMillis(), "test".getBytes());
        }

        final FileQueueImpl readQ = new FileQueueImpl(tmpDir.getAbsolutePath(), queueName, takableWhenOlderThanXMillis,
            takableWhenLargerThanXSize, pageSize, Integer.MAX_VALUE, true, pending);

        // check getSize() is correct
        long start = System.currentTimeMillis();
        while (readQ.getSize(PhasedQueueConstants.ENQUEUED) < numberOfMessages && (System.currentTimeMillis() - start < 120000)) {
            Thread.sleep(100);
        }
        AssertJUnit.assertEquals("Incorrect size", numberOfMessages, readQ.getSize(PhasedQueueConstants.ENQUEUED));

        // now start consuming
        int numConsumer = 10;
        final long sleepForNMillisOnEmptyConsume = 1;
        final List<String> consumed = new LinkedList<>();
        final CountDownLatch l = new CountDownLatch(numberOfMessages);
        final AtomicLong consumedCount = new AtomicLong();
        for (int i = 0; i < numConsumer; i++) {
            Thread thread = new Thread("consumer " + i) {
                @Override
                public void run() {
                    while (true) {
                        try {
                            List<PhasedQueueEntry> batch = readQ.consume(PhasedQueueConstants.ENQUEUED,
                                PhasedQueueConstants.CONSUMED, 1, Long.MAX_VALUE);
                            consumedCount.addAndGet(batch.size());
                            if (batch.isEmpty()) {
                                UtilThread.sleep(sleepForNMillisOnEmptyConsume);
                            } else {

                                for (PhasedQueueEntry b : batch) {
                                    String data = new String(b.getEntry());
                                    consumed.add(data);
                                    AssertJUnit.assertEquals("Incorrect data", "test", data);
                                    b.processed();
                                    l.countDown();
                                }
                            }
                            if (consumed.size() == numberOfMessages) {
                                return;
                            }

                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            };
            thread.start();
            // System.out.println("started consumer thread "+i);
        }

    }

    @Test (groups = "slow")
    public void testBouncingQueueOverAndOverAgain() throws Exception {
        System.out.println("---testAbortedConsumingFollowedByARestart---");
        MutableLong pending = new MutableLong();
        long takableWhenOlderThanXMillis = 1;
        long takableWhenLargerThanXSize = 1;
        final int numberOfMessages = 1000000;
        int pageSize = 1024 * 1024 * 1024;
        String queueName = "test";
        File tmpDir = getTempDir();

        final FileQueueImpl writeQ = new FileQueueImpl(tmpDir.getAbsolutePath(), queueName,
            takableWhenOlderThanXMillis, takableWhenLargerThanXSize, pageSize, Integer.MAX_VALUE, true, pending);

        // add a lot of messages
        System.out.println("enqueue " + numberOfMessages);
        for (int i = 0; i < numberOfMessages; i++) {
            writeQ.add(PhasedQueueConstants.ENQUEUED, System.currentTimeMillis(), "test".getBytes());
        }

        final long sleepForNMillisOnEmptyConsume = 1;
        int consumesPerBouncy = numberOfMessages / 10;
        final AtomicLong consumedCount = new AtomicLong();
        int batchSize = 100;
        while (consumedCount.get() < numberOfMessages) {

            final FileQueueImpl readQ = new FileQueueImpl(tmpDir.getAbsolutePath(), queueName, takableWhenOlderThanXMillis,
                takableWhenLargerThanXSize, pageSize, Integer.MAX_VALUE, true, pending);

            System.out.println("starting to consume " + consumesPerBouncy);
            // consume some of the messages
            final AtomicLong partialConsume = new AtomicLong();
            while (partialConsume.get() < consumesPerBouncy) {
                try {
                    List<PhasedQueueEntry> batch = readQ.consume(PhasedQueueConstants.ENQUEUED,
                        PhasedQueueConstants.CONSUMED, batchSize, Long.MAX_VALUE);
                    if (batch.isEmpty()) {
                        UtilThread.sleep(sleepForNMillisOnEmptyConsume);
                    } else {
                        for (PhasedQueueEntry b : batch) {
                            b.processed();
                            consumedCount.incrementAndGet();
                            long c = partialConsume.incrementAndGet();
                            if (c >= consumesPerBouncy) {
                                break;
                            }
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            System.out.println("stopped consuming after " + consumedCount.get());

        }

        System.out.println("All Done! Total consumed:" + consumedCount.get() + " vs " + numberOfMessages);
        AssertJUnit.assertEquals(consumedCount.get(), numberOfMessages);

    }

    public void removeBytesFromFile(File f, int offset, int bytesToRemove) throws IOException {
        File tmp = File.createTempFile("corrupted", "");
        OutputStream os = null;
        FileInputStream is = null;
        try {
            os = new FileOutputStream(tmp);
            is = new FileInputStream(f);
            int count = 0;
            while (count < offset) {
                os.write(is.read());
                count++;
            }
            count = 0;
            while (count < bytesToRemove) {
                is.read();
                count++;
            }
            int i;
            while ((i = is.read()) != -1) {
                os.write(i);
            }
            os.flush();
        } catch (Exception e) {
            if (os != null) {
                os.close();
            }
            if (is != null) {
                is.close();
            }
        }
        f.delete();
        FileUtils.copyFile(tmp, f);

    }

    @Test (enabled = false)
    public void testItemsRemainOnQueueUntilMarkedAsProcessed() throws Exception {
        File tmpDir = getTempDir();
        boolean deleteQueueFilesOnExit = false;
        boolean takeFullQueuesOnly = false;
        final String QUEUE_NAME = "phased-test-q";
        long takableWhenCreationTimestampIsOlderThanXMillis = 0;
        long takableWhenLastAppendedIsOlderThanXMillis = 0;
        long takableWhenLargerThanXSize = 10;
        MutableLong pending = new MutableLong();

        FileQueueImpl queue = new FileQueueImpl(tmpDir.getAbsolutePath(),
            QUEUE_NAME, takableWhenCreationTimestampIsOlderThanXMillis,
            takableWhenLastAppendedIsOlderThanXMillis,
            takableWhenLargerThanXSize,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            deleteQueueFilesOnExit,
            pending,
            takeFullQueuesOnly);

        queue.add(PhasedQueueConstants.ENQUEUED, new Date().getTime(), "one".getBytes());
        queue.add(PhasedQueueConstants.ENQUEUED, new Date().getTime(), "two".getBytes());
        queue.add(PhasedQueueConstants.ENQUEUED, new Date().getTime(), "three".getBytes());

        List<PhasedQueueEntry> queueEntries = queue.consume(PhasedQueueConstants.ENQUEUED, PhasedQueueConstants.ENQUEUED, 3, Long.MAX_VALUE);
        assertEquals(new String(queueEntries.get(0).getEntry()), "one");
        assertEquals(new String(queueEntries.get(1).getEntry()), "two");
        assertEquals(new String(queueEntries.get(2).getEntry()), "three");

        // we never marked the entries as processed, so when we get another batch, we should get the same items.
        queueEntries = queue.consume(PhasedQueueConstants.ENQUEUED, PhasedQueueConstants.ENQUEUED, 3, Long.MAX_VALUE);
        assertEquals(new String(queueEntries.get(0).getEntry()), "one");
        assertEquals(new String(queueEntries.get(1).getEntry()), "two");
        assertEquals(new String(queueEntries.get(2).getEntry()), "three");
    }

    @Test (enabled = false)
    public void testItemsRemovedFromQueueAfterMarkedAsProcessed() throws Exception {
        File tmpDir = getTempDir();
        boolean deleteQueueFilesOnExit = false;
        boolean takeFullQueuesOnly = false;
        final String QUEUE_NAME = "phased-test-q";
        long takableWhenCreationTimestampIsOlderThanXMillis = 0;
        long takableWhenLastAppendedIsOlderThanXMillis = 0;
        long takableWhenLargerThanXSize = 10;
        MutableLong pending = new MutableLong();

        FileQueueImpl queue = new FileQueueImpl(tmpDir.getAbsolutePath(),
            QUEUE_NAME, takableWhenCreationTimestampIsOlderThanXMillis,
            takableWhenLastAppendedIsOlderThanXMillis,
            takableWhenLargerThanXSize,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            deleteQueueFilesOnExit,
            pending,
            takeFullQueuesOnly);

        queue.add(PhasedQueueConstants.ENQUEUED, new Date().getTime(), "one".getBytes());
        queue.add(PhasedQueueConstants.ENQUEUED, new Date().getTime(), "two".getBytes());
        queue.add(PhasedQueueConstants.ENQUEUED, new Date().getTime(), "three".getBytes());

        List<PhasedQueueEntry> queueEntries = queue.consume(PhasedQueueConstants.ENQUEUED, PhasedQueueConstants.ENQUEUED, 3, Long.MAX_VALUE);
        assertEquals(new String(queueEntries.get(0).getEntry()), "one");
        assertEquals(new String(queueEntries.get(1).getEntry()), "two");
        assertEquals(new String(queueEntries.get(2).getEntry()), "three");

        queueEntries.get(1).processed();

        // we never marked the entries as processed, so when we get another batch, we should get the same items.
        queueEntries = queue.consume(PhasedQueueConstants.ENQUEUED, PhasedQueueConstants.ENQUEUED, 3, Long.MAX_VALUE);
        assertEquals(new String(queueEntries.get(0).getEntry()), "one");
        assertEquals(new String(queueEntries.get(1).getEntry()), "three");
    }

    @Test
    public void testItemsMarkedFaileStayInQueue() throws Exception {
        File tmpDir = getTempDir();
        boolean deleteQueueFilesOnExit = false;
        boolean takeFullQueuesOnly = false;
        final String QUEUE_NAME = "phased-test-q";
        long takableWhenCreationTimestampIsOlderThanXMillis = 0;
        long takableWhenLastAppendedIsOlderThanXMillis = 0;
        long takableWhenLargerThanXSize = 10;
        MutableLong pending = new MutableLong();

        FileQueueImpl queue = new FileQueueImpl(tmpDir.getAbsolutePath(),
            QUEUE_NAME, takableWhenCreationTimestampIsOlderThanXMillis,
            takableWhenLastAppendedIsOlderThanXMillis,
            takableWhenLargerThanXSize,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            deleteQueueFilesOnExit,
            pending,
            takeFullQueuesOnly);

        // fails intermittently on the build server. we have the takable*Millis set to zero, but under the hood, the
        // queue doesn't treat that properly (uses < vs <=), and essentially requires the age to be at least 1ms.
        Thread.sleep(1);

        queue.add(PhasedQueueConstants.ENQUEUED, new Date().getTime(), "one".getBytes());
        queue.add(PhasedQueueConstants.ENQUEUED, new Date().getTime(), "two".getBytes());
        queue.add(PhasedQueueConstants.ENQUEUED, new Date().getTime(), "three".getBytes());

        List<PhasedQueueEntry> queueEntries = queue.consume(PhasedQueueConstants.ENQUEUED, PhasedQueueConstants.ENQUEUED, 3, Long.MAX_VALUE);
        assertEquals(new String(queueEntries.get(0).getEntry()), "one");
        assertEquals(new String(queueEntries.get(1).getEntry()), "two");
        assertEquals(new String(queueEntries.get(2).getEntry()), "three");

        queueEntries.get(0).failed(PhasedQueueConstants.ENQUEUED);
        queueEntries.get(1).failed(PhasedQueueConstants.ENQUEUED);
        queueEntries.get(2).failed(PhasedQueueConstants.ENQUEUED);

        // failed entries should be put back on the queue, so when we get another batch, we should get the same items.
        queueEntries = queue.consume(PhasedQueueConstants.ENQUEUED, PhasedQueueConstants.ENQUEUED, 3, Long.MAX_VALUE);
        assertEquals(new String(queueEntries.get(0).getEntry()), "one");
        assertEquals(new String(queueEntries.get(1).getEntry()), "two");
        assertEquals(new String(queueEntries.get(2).getEntry()), "three");
    }

    private File getTempDir() throws IOException {
        File tmpFile = File.createTempFile("test", "");
        File tmpDir = new File(tmpFile.getParentFile(), tmpFile.getName() + "test");
        tmpDir.mkdir();
        tmpDir.deleteOnExit();
        return tmpDir;
    }
}
