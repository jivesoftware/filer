/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.store;

import com.jivesoftware.os.jive.utils.base.util.UtilPushback;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author jonathan
 */
public class TwoPhasedFileQueueImpl {

    private static final MetricLogger logger = MetricLoggerFactory.getLogger();

    enum State {

        CLOSED,
        CLOSING,
        OFF, // initial state
        OPENING,
        OPENED,
        DISPOSED // once in this state instance if no longer useable!
    }
    final private Object appendLock = new Object();
    private FileQueue appendingTo;
    private List<FileQueue> fullQueues = new LinkedList<>();
    private Set<TookPhasedFileQueue> takens = new HashSet<>();
    final private String pathToQueueFiles;
    final private String queueName;
    final private long maxQueueLength;
    final private int pushbackAtQueueSize;
    private State state = State.OFF;
    private boolean deleteQueueFilesOnExit = false; // primarily used for testing
    public AtomicLong approximateCount;
    private boolean hardFlush = false; // todo expose to config
    private MutableLong pending;
    private boolean takeFullQueuesOnly = false;

    public TwoPhasedFileQueueImpl(String pathToQueueFiles, String queueName, long maxQueueLength, int pushbackAtQueueSize,
        boolean deleteQueueFilesOnExit, MutableLong pending) {
        this(pathToQueueFiles, queueName, maxQueueLength, pushbackAtQueueSize, deleteQueueFilesOnExit, pending, false);
    }

    public TwoPhasedFileQueueImpl(String pathToQueueFiles, String queueName, long maxQueueLength, int pushbackAtQueueSize,
        boolean deleteQueueFilesOnExit, MutableLong pending, boolean takeFullQueuesOnly) {
        this.pathToQueueFiles = pathToQueueFiles;
        this.queueName = queueName;
        this.maxQueueLength = maxQueueLength;
        this.pushbackAtQueueSize = pushbackAtQueueSize;
        this.deleteQueueFilesOnExit = deleteQueueFilesOnExit;
        this.takeFullQueuesOnly = takeFullQueuesOnly;
        this.approximateCount = new AtomicLong();
        this.pending = pending;
    }

    public String key() {
        return pathToQueueFiles + ":" + queueName;
    }

    public long length() {
        long totalLength = 0;
        synchronized (appendLock) {
            for (FileQueue queue : fullQueues) {
                totalLength += queue.length();
            }
            if (appendingTo != null) {
                totalLength += appendingTo.length();
            }
        }
        return totalLength;
    }

    /**
     * should only be called once
     *
     * @param makeItThisMode
     * @throws IOException
     */
    public void init(long makeItThisMode) throws IOException {
        synchronized (appendLock) {
            if (!(state == State.CLOSED || state == State.OFF)) {
                throw new RuntimeException("queue not in a valid state to open opened state=" + state);
            }
            state = State.OPENING;
            File queueFolder = queueFolder();
            ensureDirectory(new File(queueFolder, "."));
            if (!queueFolder.exists()
                || !queueFolder.canWrite()
                || !queueFolder.canRead()) {
                logger.error("we have a filesystem permissions issue!");
                throw new IOException("invalid permissions exsist:" + queueFolder.exists()
                    + " canwrite:" + queueFolder.canWrite() + " canread:" + queueFolder.canWrite()
                    + " path:" + pathToQueueFiles + " queueName:" + queueName + " queueFolder:" + queueFolder);
            }
            populateFullQueuesInBackground(queueFolder.listFiles(), makeItThisMode);
            appendingTo = new FileQueue(newQueueFile(queueFolder), hardFlush);

            state = State.OPENED;
        }
    }

    public Exception ensureDirectory(File _file) {
        if (_file == null) {
            return null;
        }
        try {
            if (_file.exists()) {
                return null;
            }
            File parent = _file.getParentFile();
            if (parent != null) {
                parent.mkdirs();
            }
            return null;
        } catch (Exception x) {
            return x;
        }
    }

    public void open() throws IOException {
        synchronized (appendLock) {
            if (state == State.OPENED) {
                return; // already open
            }
            if (!(state == State.CLOSED || state == State.OFF)) {
                throw new RuntimeException("queue not in a valid state to open opened state=" + state);
            }
            state = State.OPENED;
        }
    }

    private Thread populateFullQueuesInBackground(final File[] allQueueFiles, final long makeItThisMode) {
        if (allQueueFiles == null || allQueueFiles.length == 0) {
            logger.debug("No files so we don't have anything to populate anything.");
            return null;
        }
        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                readExistingQueueFiles(allQueueFiles, makeItThisMode);
            }
        });
        th.start();
        return th;
    }

    private void readExistingQueueFiles(File[] allQueueFiles, long makeItThisMode) {
        Arrays.sort(allQueueFiles, new Comparator<File>() {

            @Override
            public int compare(File o1, File o2) {
                if (o1.getName().endsWith("corrupt")) {
                    return 0;
                }
                return new UniqueOrderableFileName(o1.getName()).getOrderId().compareTo(new UniqueOrderableFileName(o2.getName()).getOrderId());
            }
        });

        logger.info("Init Populating " + allQueueFiles.length + " files into full queues.");
        for (File queueFile : allQueueFiles) {
            if (queueFile.getName().endsWith("corrupt")) {
                continue;
            }
            logger.info(new UniqueOrderableFileName(queueFile.getName()).getOrderId() + " " + queueFile);
            FileQueue fullQueue = new FileQueue(queueFile, hardFlush);
            logger.info("Counting " + queueFile);
            try {
                long count = fullQueue.bruteForceCount(makeItThisMode, true);
                logger.info("Counted " + queueFile + " count=" + count);
                if (count > 0) {
                    fullQueue.close();
                    synchronized (appendLock) {
                        //older queues should be at the beginning.
                        List<FileQueue> newList = new LinkedList<>();
                        newList.add(fullQueue);
                        newList.addAll(fullQueues);
                        fullQueues = newList;
                    }
                    approximateCount.addAndGet(count);
                }
            } catch (Exception x) {
                logger.error("while counting file=" + queueFile + " " + x.toString(), x);
            }
        }
        logger.info("Populated " + allQueueFiles.length + ".");
        logger.info("Startup queue depth=" + approximateCount.get());
    }

    public void append(long timestamp, byte[] append) throws IOException {
        if (append == null) {
            return;
        }

        synchronized (appendLock) {
            long got = approximateCount.get();
            pending.setValue(got);
            UtilPushback.queueDepthPushable(queueName, got, pushbackAtQueueSize);

            if (state != State.OPENED) {
                open();
            }
            if (state != State.OPENED) {
                throw new RuntimeException("cannot write to an unopened queue");
            }
            try {
                appendingTo.append(timestamp, append);
                approximateCount.incrementAndGet();
            } catch (Exception x) {
                logger.error("Failed to append. Will attempt to recover", x);
                File f = appendingTo.getFile();
                long free = f.getParentFile().getFreeSpace();
                if (free < maxQueueLength * 2) {
                    logger.error("we are out of disk space.");
                    throw new IOException("out of disk space");
                }
                if (!f.canWrite()) {
                    logger.error("we don't have permissions to write to " + f);
                    throw new IOException("invalid permissions");
                }
                synchronized (appendingTo.getIoLock()) {
                    if (appendingTo.getState() == FileQueue.State.ERROR_WRITING) {
                        try {
                            // try to save queue for later inspection
                            if (appendingTo.length() > 0) {
                                try {
                                    appendingTo.close();
                                } catch (Exception xx) {
                                    // swallow it at least we tried
                                }
                                appendingTo.corruptionDetected();
                            }
                            appendingTo = new FileQueue(newQueueFile(queueFolder()), hardFlush);
                        } catch (Exception xx) {
                            logger.error("Failed to delete failing queue :(", xx);
                        }
                    }
                }
                throw new IOException("unable to write to filesystem", x);
            }

            if (appendingTo.length() > maxQueueLength) {
                appendingTo.close();
                fullQueues.add(appendingTo);
                appendingTo.close();
                appendingTo = new FileQueue(newQueueFile(queueFolder()), hardFlush);
            }
        }
    }

    public boolean isEmpty() {
        synchronized (appendLock) {
            long got = approximateCount.get();
            pending.setValue(got);
            if (fullQueues.isEmpty()) {
                if (appendingTo == null) {
                    return true;
                }
                return appendingTo.length() == 0;
            } else {
                return false;
            }
        }
    }

    public boolean isTakeFullQueuesOnly() {
        return takeFullQueuesOnly;
    }

    public TookPhasedFileQueue take(long ifLastAppendedIsOlderThanTimestampMillis) throws IOException {
        return take(-1, ifLastAppendedIsOlderThanTimestampMillis);
    }

    TookPhasedFileQueue take(long ifCreationTimestampIsOlderThanTimestampMillis, long ifLastAppendedIsOlderThanTimestampMillis) throws IOException {
        synchronized (appendLock) {
            long got = approximateCount.get();
            pending.setValue(got);
            if (state != State.OPENED) {
                open();
            }
            if (state != State.OPENED) {
                throw new RuntimeException("cannot take from an unopened queue");
            }
            if (fullQueues.isEmpty()) {
                if (takeFullQueuesOnly) {
                    logger.debug("Taking nothing because set to takeFullQueuesOnly");
                    TookPhasedFileQueue took = new TookPhasedFileQueue(this, TookPhasedFileQueue.State.BUSY, null, null, approximateCount, pending);
                    return took;
                } else if (appendingTo.length() == 0) {
                    logger.debug("Taking nothing because appendingTo.length() == 0 ");
                    TookPhasedFileQueue took = new TookPhasedFileQueue(this, TookPhasedFileQueue.State.EMPTY, null, null, approximateCount, pending);
                    return took;
                } else {
                    if ((appendingTo.creationTimestamp() < ifCreationTimestampIsOlderThanTimestampMillis)
                        || appendingTo.lastAppendTimestamp() < ifLastAppendedIsOlderThanTimestampMillis) {
                        logger.debug("Taking active queue!");
                        appendingTo.close();
                        FileQueue taken = appendingTo;
                        appendingTo = new FileQueue(newQueueFile(queueFolder()), hardFlush);
                        TookPhasedFileQueue took = new TookPhasedFileQueue(this, TookPhasedFileQueue.State.CONSUMEABLE,
                            new UniqueOrderableFileName(taken.getFile().getName()), taken, approximateCount, pending);
                        try {
                            taken.close(); // nested locks
                        } catch (Exception x) {
                            logger.error("Failed during take", x);
                            // todo should we try to remove the file
                            return new TookPhasedFileQueue(this, TookPhasedFileQueue.State.FAILED, null, taken, approximateCount, pending);
                        }
                        takens.add(took);
                        return took;
                    } else {
                        TookPhasedFileQueue took = new TookPhasedFileQueue(this, TookPhasedFileQueue.State.BUSY, null, null, approximateCount, pending);
                        return took;
                    }
                }
            } else {
                logger.debug("Taking a full queue!");
                FileQueue taken = fullQueues.remove(0);
                TookPhasedFileQueue took = new TookPhasedFileQueue(this, TookPhasedFileQueue.State.CONSUMEABLE,
                    new UniqueOrderableFileName(taken.getFile().getName()), taken, approximateCount, pending);
                takens.add(took);
                return took;
            }
        }
    }

    public void processed(TookPhasedFileQueue processed, boolean closeAndDisposeIfEmpty) {
        synchronized (appendLock) {
            long got = approximateCount.get();
            pending.setValue(got);
            boolean wasRemoved = takens.remove(processed);
            logger.debug(processed + " was removed =" + wasRemoved + " from takens!");
            try {
                if (wasRemoved) {
                    processed.took().delete();
                    if (closeAndDisposeIfEmpty) {
                        if (fullQueues.isEmpty() && takens.isEmpty()) {
                            closeAndDisposeIfEmtpy();
                        }
                    }
                }
            } catch (Exception x) {
                logger.error("failed to remove queue file that was processed", x);
            }
        }
    }

    public void clear() {
        synchronized (appendLock) {
            // clean out available up unused queues
            for (FileQueue q : fullQueues) {
                q.close();
                q.delete();
            }
            fullQueues.clear();
            // clean out currenlty appending to
            if (appendingTo != null) {
                appendingTo.close();
                appendingTo.delete();
                appendingTo = new FileQueue(newQueueFile(queueFolder()), hardFlush);
            }
            approximateCount.set(0);
        }
    }

    /**
     *
     * @return true if closed
     */
    boolean closeAndDisposeIfEmtpy() {
        synchronized (appendLock) {
            long got = approximateCount.get();
            pending.setValue(got);

            if (fullQueues.isEmpty() && takens.isEmpty() && isEmpty()) {
                FileQueue stackCopy = appendingTo;
                close();
                if (stackCopy != null && stackCopy.length() == 0) {
                    File file = stackCopy.getFile();
                    if (stackCopy.delete()) {
                        logger.debug("Deleted Queue File " + file + " openFilesCount=" + FileQueue.openFilesCount);
                        File folder = file.getParentFile();
                        if (folder.delete()) {
                            logger.debug("Deleted Queue Folder " + folder + " openFilesCount=" + FileQueue.openFilesCount);
                            state = State.DISPOSED;
                            return true;
                        } else {
                            logger.warn("Couldn't delete Folder " + folder + " openFilesCount=" + FileQueue.openFilesCount);
                        }
                    } else {
                        logger.warn("Couldn't delete File for " + file + " openFilesCount=" + FileQueue.openFilesCount);
                    }
                }
            }
        }
        return false;
    }

    public void close() {
        synchronized (appendLock) {
            long got = approximateCount.get();
            pending.setValue(got);

            if (!takens.isEmpty()) {
                throw new RuntimeException("Cannot when there are outstanding takes!");
            }
            state = State.CLOSING;
            if (appendingTo != null) {
                appendingTo.close();
            }
            appendingTo = null;
            state = State.CLOSED;
        }
        logger.debug("Closed " + key() + " openFilesCount=" + FileQueue.openFilesCount);
    }

    private File queueFolder() {
        File queueFolder = new File(pathToQueueFiles + File.separator + queueName);
        if (deleteQueueFilesOnExit) {
            queueFolder.deleteOnExit();
        }
        return queueFolder;
    }

    private File newQueueFile(File queueFolder) {
        File queueFile = new File(queueFolder, UniqueOrderableFileName.createOrderableFileName().toString());
        if (deleteQueueFilesOnExit) {
            queueFile.deleteOnExit();
        }
        return queueFile;
    }
}
