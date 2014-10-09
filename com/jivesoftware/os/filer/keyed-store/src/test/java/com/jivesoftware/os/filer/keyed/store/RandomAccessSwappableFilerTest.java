package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.io.FilerIO;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RandomAccessSwappableFilerTest {

    private TestRunnable r1;
    private TestRunnable r2;
    private TestRunnable r3;

    @BeforeMethod
    public void setUp() throws Exception {
        RandomAccessSwappableFiler swappableFiler = new RandomAccessSwappableFiler(File.createTempFile("testLock", "tmp"));

        synchronized (swappableFiler.lock()) {
            swappableFiler.sync();
            swappableFiler.seek(0);
            FilerIO.writeInt(swappableFiler, 0, "int");
        }

        r1 = new TestRunnable(swappableFiler, 1);
        r2 = new TestRunnable(swappableFiler, 2);
        r3 = new TestRunnable(swappableFiler, 3);
    }

    @Test(enabled = false, description = "Disabled due to reliance on timeout, used to validate locking fix")
    public void testLock() throws Exception {
        new Thread(r1).start();
        new Thread(r2).start();
        new Thread(r3).start();

        // r1 gets lock and starts swapping
        r1.latchTryLock.countDown();
        r1.latchAcquiredLock.await();
        r1.latchStartSwap.countDown();

        // r2 requests lock
        r2.latchTryLock.countDown();
        if (r2.latchAcquiredLock.await(100, TimeUnit.MILLISECONDS)) {
            Assert.fail("r1 should still be holding the lock");
        }

        // r1 commits and unlocks
        r1.latchCommitSwap.await();
        r1.latchReleaseLock.countDown();

        // r2 gets lock
        r2.latchAcquiredLock.await();
        r2.latchStartSwap.countDown();

        // r3 requests lock
        r3.latchTryLock.countDown();
        if (r3.latchAcquiredLock.await(100, TimeUnit.MILLISECONDS)) {
            Assert.fail("r2 should still be holding the lock");
        }
    }

    private static class TestRunnable implements Runnable {

        private final RandomAccessSwappableFiler swappableFiler;
        private final int value;

        private final CountDownLatch latchTryLock = new CountDownLatch(1);
        private final CountDownLatch latchAcquiredLock = new CountDownLatch(1);
        private final CountDownLatch latchStartSwap = new CountDownLatch(1);
        private final CountDownLatch latchCommitSwap = new CountDownLatch(1);
        private final CountDownLatch latchReleaseLock = new CountDownLatch(1);

        private TestRunnable(RandomAccessSwappableFiler swappableFiler, int value) {
            this.swappableFiler = swappableFiler;
            this.value = value;
        }

        @Override
        public void run() {
            try {
                latchTryLock.await();
                synchronized (swappableFiler.lock()) {
                    latchAcquiredLock.countDown();
                    latchStartSwap.await();

                    swappableFiler.sync();

                    SwappingFiler swap = swappableFiler.swap(4);
                    swap.seek(0);
                    FilerIO.writeInt(swap, value, "int");
                    swap.commit();

                    latchCommitSwap.countDown();
                    latchReleaseLock.await();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
