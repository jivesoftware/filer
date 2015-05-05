/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.filer.queue.store;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan
 */
public class FileQueueTest {

    private File file1Hard = null;
    private File file1Soft = null;
    private File file2 = null;

    @BeforeMethod
    public void createFiles() throws Exception {
        file1Hard = File.createTempFile("fileQueueTestsHard", null);
        file1Hard.deleteOnExit();

        file1Soft = File.createTempFile("fileQueueTestsSoft", null);
        file1Soft.deleteOnExit();

        file2 = File.createTempFile("fileQueueTests", null);
        file2.deleteOnExit();
    }

    //@Ignore
    @Test
    public void testAppend() {
        Random random = new Random();
        // test hard flush
        FileQueue queue1 = new FileQueue(file1Hard, true);
        int expectFileLenth = 0;
        for (int i = 0; i < 10; i++) {
            byte[] append = randomLowerCaseAlphaBytes(1234, 1 + random.nextInt(512));
            expectFileLenth += (FileQueue.headerSize + append.length + FileQueue.endOfDataMarkerSize);
            queue1.append(0, append);
        }
        queue1.close();
        AssertJUnit.assertTrue("is=" + file1Hard.length() + " vs expected=" + expectFileLenth, file1Hard.length() == expectFileLenth);

        AssertJUnit.assertTrue("failed to delete " + queue1, queue1.delete());

        // test soft flush
        queue1 = new FileQueue(file1Soft, false);
        expectFileLenth = 0;
        for (int i = 0; i < 10; i++) {
            byte[] append = randomLowerCaseAlphaBytes(1234, 1 + random.nextInt(512));
            expectFileLenth += (FileQueue.headerSize + append.length + FileQueue.endOfDataMarkerSize);
            queue1.append(0, append);
        }
        queue1.close();
        AssertJUnit.assertTrue("is=" + file1Soft.length() + " vs expected=" + expectFileLenth, file1Soft.length() == expectFileLenth);

        AssertJUnit.assertTrue("failed to delete " + queue1, queue1.delete());
    }

    //@Ignore
    @Test
    public void testAppendAndRead() {
        Random random = new Random();
        FileQueue queue1 = new FileQueue(file1Hard, true);
        int expectFileLenth = 0;
        final List<byte[]> wrote = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            byte[] append = randomLowerCaseAlphaBytes(1234, 1 + random.nextInt(512));
            expectFileLenth += (FileQueue.headerSize + append.length + FileQueue.endOfDataMarkerSize);
            queue1.append(0, append);
            wrote.add(append);
        }
        queue1.close();
        AssertJUnit.assertTrue("is=" + file1Hard.length() + " vs expected=" + expectFileLenth, file1Hard.length() == expectFileLenth);

        queue1.read(0, 0, read -> {
            if (read == null) {
                return read;
            }
            byte[] wasWritten = wrote.remove(0);
            AssertJUnit.assertArrayEquals(read.getEntry(), wasWritten);
            return read;
        });
        queue1.close();

        AssertJUnit.assertTrue("failed to delete " + queue1, queue1.delete());

    }

    public byte[] randomLowerCaseAlphaBytes(long _longSeed, int _length) {
        byte[] bytes = new byte[_length];
        fill(_longSeed, bytes, 0, _length, 97, 122); // 97 122 lowercase a to z ascii
        return bytes;
    }

    public static void fill(long _longSeed, byte[] _fill, int _offset, int _length, int _min, int _max) {
        Random random = new Random(_longSeed);
        for (int i = _offset; i < _offset + _length; i++) {
            _fill[i] = (byte) (_min + random.nextInt(_max - _min));
        }
    }
}
