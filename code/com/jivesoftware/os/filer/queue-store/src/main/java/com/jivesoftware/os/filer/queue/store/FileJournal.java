/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.filer.queue.store;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.base.util.UtilThread;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.mutable.MutableBoolean;

/**
 *
 * @author jonathan
 */
abstract public class FileJournal<V> {

    abstract public V toInstance(ByteBuffer bytes) throws IOException;

    abstract public ByteBuffer toBytes(V v) throws IOException;

    abstract public long toTimestamp(V v);

    abstract public String toJournalName(V v);
    static private final MetricLogger logger = MetricLoggerFactory.getLogger();
    private final String pathToJournal;
    private final long maxPageSize;
    private final long autoCloseAfterNMillis;
    private final ConcurrentHashMap<String, Journal> journals;

    public FileJournal(String pathToJournal, long maxPageSize, long autoCloseAfterNMillis) {
        this.pathToJournal = pathToJournal;
        this.maxPageSize = maxPageSize;
        this.autoCloseAfterNMillis = autoCloseAfterNMillis;
        journals = new ConcurrentHashMap<>();
    }

    private File queueFolder(String systemId) {
        File queueFolder = new File(pathToJournal + File.separator + systemId);
        return queueFolder;
    }

    public void removeJournals(String systemId) {
        if (systemId == null) {
            return;
        }
        File queueFolder = queueFolder(systemId);
        if (queueFolder == null) {
            return;
        }
        File[] files = queueFolder.listFiles();
        if (files != null) {
            for (File j : files) {
                if (j == null) {
                    continue;
                }
                if (j.isFile()) {
                    j.delete();
                }
            }
        }
    }

    public void getAll(String systemId, final CallbackStream<V> callbackStream) throws Exception {
        File queueFolder = queueFolder(systemId);
        ensureDirectory(queueFolder);
        File[] allPages = queueFolder.listFiles();
        if (allPages == null || allPages.length == 0) {
            callbackStream.callback(null); //EOS
            return;
        }
        Arrays.sort(allPages, new Comparator<File>() {

            @Override
            public int compare(File o1, File o2) {
               return new UniqueOrderableFileName(o1.getName()).getOrderId().compareTo(new UniqueOrderableFileName(o2.getName()).getOrderId());
            }
        });
        final MutableBoolean callerStopped = new MutableBoolean(false);
        for (File page : allPages) {
            FileQueue journal = new FileQueue(page, false);
            journal.read(0, 0, new CallbackStream<FileQueueEntry>() {
                @Override
                public FileQueueEntry callback(FileQueueEntry value) throws Exception {
                    if (value == null) {
                        return value;
                    }
                    V vector = toInstance(ByteBuffer.wrap(value.getEntry()));
                    V response = callbackStream.callback(vector);
                    if (response == null) {
                        callerStopped.setValue(true);
                        return null; // stops the stream
                    }
                    return value;
                }
            });
            if (callerStopped.isTrue()) {
                break;
            }
        }
        callbackStream.callback(null); // EOS
    }

    private Journal getQueue(String systemId) {
        Journal got = journals.get(systemId);
        if (got != null) {
            return got;
        }
        got = new Journal(systemId, maxPageSize, autoCloseAfterNMillis); // expose to config / constructor
        Journal had = journals.putIfAbsent(systemId, got);
        if (had != null) {
            return had; // someone else beat up to creating this queue
        }
        return got;
    }

    public void add(V add) throws Exception {
        if (add == null) {
            return;
        }
        Journal journal = getQueue(toJournalName(add));
        journal.append(toTimestamp(add), toBytes(add));
    }

    public void closeAll() {
        for (Journal journal : journals.values()) {
            journal.close();
        }
    }

    class Journal {

        private final String systemId;
        private final long maxPageSize;
        private final long autoCloseAfterNMillis;
        private final Object appendLock = new Object();
        private SelfClosingJournal appendingTo;

        Journal(String systemId, long maxPageSize, long autoCloseAfterNMillis) {
            this.systemId = systemId;
            this.maxPageSize = maxPageSize;
            this.autoCloseAfterNMillis = autoCloseAfterNMillis;
        }

        public void append(long timestamp, ByteBuffer append) throws IOException {
            if (append == null) {
                return;
            }
            synchronized (appendLock) {
                if (appendingTo == null) {
                    appendingTo = new SelfClosingJournal();
                }
                while (true) {
                    boolean appended = appendingTo.append(timestamp, append.array());
                    if (appended) {
                        break;
                    } else {
                        appendingTo = new SelfClosingJournal();
                    }
                }
            }
        }

        public void close() {
            synchronized (appendLock) {
                if (appendingTo != null) {
                    appendingTo.close();
                }
            }
        }

        class SelfClosingJournal {

            private FileQueue appendingTo;
            private long lastTimestamp;
            private final Object appendLock = new Object();

            SelfClosingJournal() {
                appendingTo = new FileQueue(newQueueFile(queueFolder(systemId)), false);
                lastTimestamp = System.currentTimeMillis();
                startAutoClosingThread();
            }

            private void startAutoClosingThread() {
                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        while (appendingTo != null) {
                            synchronized (appendLock) {
                                if (appendingTo == null) {
                                    return;
                                }
                                long elapse = System.currentTimeMillis() - lastTimestamp;
                                if (elapse > autoCloseAfterNMillis) {
                                    logger.debug("Auto closing " + appendingTo.toString());
                                    close();
                                    return;
                                }
                            }
                            UtilThread.sleep(autoCloseAfterNMillis / 2);
                        }
                    }
                };
                thread.start();
            }

            public void close() {
                synchronized (appendLock) {
                    if (appendingTo == null) {
                        return;
                    }
                    appendingTo.close();
                    appendingTo = null;
                }
            }

            /**
             * Returns false
             *
             * @param timestamp
             * @param append
             * @return
             * @throws IOException
             */
            public boolean append(long timestamp, byte[] append) throws IOException {
                synchronized (appendLock) {
                    if (appendingTo == null) {
                        return false;
                    }
                    appendingTo.append(timestamp, append);
                    lastTimestamp = System.currentTimeMillis();
                    if (appendingTo.length() > maxPageSize) {
                        appendingTo.close();
                        appendingTo = null;
                        return true;
                    }
                    return true;
                }
            }

            private File newQueueFile(File queueFolder) {
                File queueFile = new File(queueFolder, UniqueOrderableFileName.createOrderableFileName().toString());
                return queueFile;
            }
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
}
