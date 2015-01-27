/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.store;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * basically a file of length,byte[], tuples
 *
 * @author jonathan
 */
public class FileQueue {

    public static final long ENQEUED = 0;
    public static final long CONSUMED = 1;
    public static final long FAILED = 2;
    public static final long REMOVED = Long.MAX_VALUE;
    private static final MetricLogger logger = MetricLoggerFactory.getLogger();
    public static long openFilesCount = 0;
    private static final int END_OF_DATA_MARKER = 0xFFFF;
    public final static int endOfDataMarkerSize = 4;
    public static final int headerSize = 8 + 8 + 8;
    public final static long ABSOLUTE_MAX_QUEUE_ENTRY_SIZE_IN_BYTES = 1024 * 1024 * 1024; // 1gb

    enum State {

        ERROR_READING,
        READING,
        OPENING_READING,
        OFF,
        OPENING_WRITING,
        WRITING,
        ERROR_WRITING,
        DELETED,
        FAILED_TO_DELETE
    }
    final private File file;
    final private Object ioLock = new Object();
    private State state = State.OFF;
    private RandomAccessFile write;
    private RandomAccessFile read;
    final private long creationTimestamp;
    private long appendedTimestamp;
    private boolean hardFlush;
    private final byte[] modeLengthAndTimestamp = new byte[headerSize];
    private final byte[] endOfDataMarker = new byte[endOfDataMarkerSize];

    public FileQueue(File file, boolean hardFlush) {
        this.file = file;
        this.hardFlush = hardFlush;
        this.creationTimestamp = System.currentTimeMillis();
    }

    /**
     * Not thread safe. use ioLock to be thread safe
     *
     * @return
     */
    State getState() {
        return state;
    }

    Object getIoLock() {
        return ioLock;
    }

    @Override
    public String toString() {
        return "FileQueue:" + file.getName();
    }

    public File getFile() {
        return file;
    }

    public long length() {
        return file.length();
    }

    public long creationTimestamp() {
        return creationTimestamp;
    }

    public long lastAppendTimestamp() {
        return appendedTimestamp;
    }

    public void ensureFileExists() throws IOException {
        ensureDirectory(file); // todo is there a better way
        if (!file.exists()) {
            logger.debug("Creating new Queue File for " + file);
            file.createNewFile();
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

    /**
     *
     * Will throw exception under the following conditions: if someone is coping this queue if some on is already reading from this queue
     *
     * @param append
     */
    public void append(long timestamp, byte[] append) {
        synchronized (ioLock) {
            RandomAccessFile writeTo = write(); // this outside of the try so it doesn't effect the state if we are making a copy
            long fp = -1;
            try {
                if (append == null) {
                    throw new RuntimeException("null entries aren't supported by this queue!");
                }
                int l = append.length;
                if (l == 0) {
                    throw new RuntimeException("zero lengthed entries aren't supported by this queue!");
                }
                if (l > ABSOLUTE_MAX_QUEUE_ENTRY_SIZE_IN_BYTES) {
                    throw new RuntimeException("entry length exceeds ABSOLUTE_MAX_QUEUE_ENTRY_SIZE_IN_BYTES=" + ABSOLUTE_MAX_QUEUE_ENTRY_SIZE_IN_BYTES);
                }
                fp = writeTo.getFilePointer(); // get current fp so that if anything goes wrong we can put it back in the right place.
                writeTo.write(FilerIO.longBytes(ENQEUED));
                writeTo.write(FilerIO.longBytes(l));
                writeTo.write(FilerIO.longBytes(timestamp));
                writeTo.write(append);
                writeTo.write(FilerIO.intBytes(END_OF_DATA_MARKER));
                if (hardFlush) {
                    writeTo.getFD().sync();
                }
                appendedTimestamp = System.currentTimeMillis();

            } catch (IOException e) {
                try {
                    writeTo.seek(fp); // something went wrong try to put the fp back in the right place.
                    writeTo.setLength(fp); // try to truncate the file
                } catch (IOException ioe) {
                    // swallow exception. oh well we tried.
                    state = State.ERROR_WRITING;
                }
                logger.error("failed to write to file=" + file + " openFilesCount=" + openFilesCount, e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     *
     * Will throw exception under the following conditions: if someone is coping this queue if some on is already writing to this queue
     *
     * @param append
     */
    public void read(long ifInThisMode, long afterReadSetToThisMode, QueueEntryStream<FileQueueEntry> stream) {

        synchronized (ioLock) {
            RandomAccessFile readFrom = read(); // this outside of the try so it doesn't effect the state if we are making a copy
            if (readFrom == null) {
                state = State.ERROR_READING;
                logger.error("failed to read from file=" + file + " openFilesCount=" + openFilesCount);
                try {
                    stream.stream(null); // denots end of stream
                } catch (Exception ex) {
                    logger.error("failed marking end of stream=", ex);
                }
                return;
            }
            try {
                while (true) {
                    FileQueueEntry fileQueueEntry = read(readFrom, ifInThisMode, afterReadSetToThisMode);
                    if (fileQueueEntry == null) {
                        break;
                    } else {
                        FileQueueEntry returned = stream.stream(fileQueueEntry);
                        if (returned == null) {
                            break;
                        }
                    }
                }
                stream.stream(null); // denots end of stream
                close();

            } catch (IOException ioex) {
                state = State.ERROR_READING;
                logger.error("failed to read from file=" + file + " openFilesCount=" + openFilesCount, ioex);
                throw new RuntimeException("Failed to close file=" + file + " openFilesCount=" + openFilesCount, ioex);
            } catch (Exception ex) {
                state = State.ERROR_READING;
                logger.error("failed to read from file=" + file + " openFilesCount=" + openFilesCount, ex);
                throw new RuntimeException("Failed to close file=" + file + " openFilesCount=" + openFilesCount, ex);
            }
        }
    }

    public FileQueueEntry readNext(long ifInThisMode, long afterReadSetToThisMode) {
        synchronized (ioLock) {
            RandomAccessFile readFrom = read(); // this outside of the try so it doesn't effect the state if we are making a copy
            if (readFrom == null) {
                return null;
            }
            try {
                logger.trace("read from " + getFile());
                return read(readFrom, ifInThisMode, afterReadSetToThisMode);
            } catch (IOException ioex) {
                state = State.ERROR_READING;
                logger.error("failed to read from file=" + file + " openFilesCount=" + openFilesCount, ioex);
                throw new RuntimeException("Failed to close file=" + file + " openFilesCount=" + openFilesCount, ioex);
            } catch (Exception ex) {
                state = State.ERROR_READING;
                logger.error("failed to read from file=" + file + " openFilesCount=" + openFilesCount, ex);
                throw new RuntimeException("Failed to close file=" + file + " openFilesCount=" + openFilesCount, ex);
            }
        }
    }

    // assumes called is holding ioLock
    private FileQueueEntry read(RandomAccessFile readFrom, long ifInThisMode, long afterReadSetToThisMode) throws IOException {
        while (true) {
            long fp = readFrom.getFilePointer();
            int r = readFrom.read(modeLengthAndTimestamp);
            if (r == -1) {
                // likely end of file
                return null;
            }
            if (r != headerSize) {
                corruptionDetected();
                return null;
            }
            long[] mlt = FilerIO.bytesLongs(modeLengthAndTimestamp);
            long mode = mlt[0];
            if (mlt[1] > ABSOLUTE_MAX_QUEUE_ENTRY_SIZE_IN_BYTES) {
                corruptionDetected();
                throw new IOException("single entry length=" + mlt[1] + " will out strip ram!");
            }

            if (fp + headerSize + mlt[1] > readFrom.length()) {
                logger.warn("encoutered premature end of file:{}", getFile());
                return null;
            }

            int l = (int) mlt[1];
            long timestamp = mlt[2];
            long beginingOfMessage = readFrom.getFilePointer();
            if (mode == ifInThisMode && ifInThisMode != afterReadSetToThisMode) {
                readFrom.seek(fp);
                readFrom.writeLong(afterReadSetToThisMode); // should we fsync?
                // todo: may want to make this configurable
                // readFrom.getFD().sync();// this is hacky but hey we want this to happen a soon as possible
                // readFrom.seek(fp); // put fp back to where it was
                readFrom.seek(beginingOfMessage);
            }
            byte[] appended = new byte[l];
            r = readFrom.read(appended);
            if (r != l) {
                corruptionDetected();
                throw new IOException("expected " + l + " bytes");
            }
            r = readFrom.read(endOfDataMarker);
            if (FilerIO.bytesInt(endOfDataMarker, 0) != END_OF_DATA_MARKER) {
                corruptionDetected();
                throw new IOException("expected END_OF_DATA_MARKER " + END_OF_DATA_MARKER + " == " + FilerIO.bytesInt(endOfDataMarker, 0));
            }

            if (mode != ifInThisMode) {
                continue; //not in desired mode so continue to next
            }
            return new FileQueueEntry(fp, timestamp, appended) {
                @Override
                public void processed() {
                    setMode(this, REMOVED);
                }

                @Override
                public void failed(long modeIfFailed) {
                    setMode(this, FAILED);
                }
            };
        }
    }

    public void corruptionDetected() {
        try {

            File f = file;
            if (f.length() == 0) {
                try {
                    file.delete();
                } catch (Exception ex) {
                    logger.error("Failed to delete corrupt file!", ex);
                }
                return;
            }

            long free = f.getFreeSpace();
            if (free < f.length() * 2) {
                logger.error("we are out of disk space.");
                throw new IOException("out of disk space");
            }
            if (!f.canWrite()) {
                logger.error("we don't have permissions to write to " + f);
                throw new IOException("invalid permissions");
            }

            File corrupt = new File(file.getAbsolutePath() + ".corrupt");
            while (corrupt.exists()) {
                corrupt = new File(corrupt.getAbsolutePath() + ".corrupt");
            }

            copyTo(file, corrupt);
            try {
                file.delete();
            } catch (Exception ex) {
                logger.error("Failed to delete corrupt file!", ex);
            }
        } catch (Exception x) {
            logger.error("Failed to make a copy of the corrupt file!", x);
            try {
                file.delete();
            } catch (Exception ex) {
                logger.error("Failed to delete corrupt file!", x);
            }
        }
    }

    public boolean copyTo(File _from, File _to) throws Exception {
        boolean fromIsDir = _from.isDirectory();
        boolean toIsDir = _to.isDirectory();

        if (fromIsDir != toIsDir) {
            throw new Exception(_from + " isn't the same type as " + _to);
        }
        if (_from.isDirectory()) {
            File[] array = _from.listFiles();
            if (array != null) {
                for (int i = 0; i < array.length; i++) {
                    File copyTo = new File(_to, array[i].getName());
                    if (array[i].isDirectory()) {
                        copyTo.mkdir();
                    }
                    copyTo(array[i], copyTo); //!!recursion

                }
            }
        } else {
            if (_to.exists()) {
                return true; // replace or skip
            }
            File parent = _to.getParentFile();
            if (parent != null) {
                parent.mkdirs();
                //_to.createNewFile();
            }
            OutputStream to;
            try (InputStream from = new FileInputStream(_from)) {
                to = new FileOutputStream(_to);
                BufferedInputStream f = new BufferedInputStream(from, 16384);
                BufferedOutputStream t = new BufferedOutputStream(to, 16384);
                int i = -1;
                while ((i = f.read()) != -1) {
                    t.write(i);
                }
                t.flush();
            }
            to.close();
        }
        return true;
    }

    public void setMode(FileQueueEntry entry, long mode) {
        setMode(entry.getFp(), mode);
    }

    private void setMode(long entryFp, long mode) {
        synchronized (ioLock) {
            if (state == State.DELETED) {
                return; // all are already gone!
            }
            RandomAccessFile readFrom = read(); // this outside of the try so it doesn't effect the state if we are making a copy
            if (readFrom == null) {
                return;
            }
            try {
                long fp = readFrom.getFilePointer();
                readFrom.seek(entryFp);
                int r = readFrom.read(modeLengthAndTimestamp);
                if (r != headerSize || r == -1) {
                    close();
                    return; // EOF
                }
                long[] mlt = FilerIO.bytesLongs(modeLengthAndTimestamp);
                mlt[0] = mode;
                readFrom.seek(entryFp);
                readFrom.write(FilerIO.longsBytes(mlt));
                // todo: may want to make this configurable
                if (hardFlush) {
                    readFrom.getFD().sync(); // this is hacky but hey we want this to happen a soon as possible
                } else {
                    // let RandomAccessFile flush when it feels like
                }
                readFrom.seek(fp); // put fp back to where it was

            } catch (IOException ioex) {
                logger.error("failed to remove entryFp=" + entryFp + " from file=" + file + " openFilesCount=" + openFilesCount, ioex);
                throw new RuntimeException("Failed to close file=" + file + " openFilesCount=" + openFilesCount, ioex);
            }
        }
    }

    /**
     * Should only be called during startup
     *
     * @return
     */
    public long bruteForceCount(long makeItThisMode, boolean removeIfCountIsZero) {
        if (file.length() == 0) {
            return 0;
        }
        long bruteForceCount = 0;
        synchronized (ioLock) {
            if (state == State.DELETED) {
                return 0; // all are already gone!
            }
            boolean closeAfterCounting = false;
            if (state == State.OFF) {
                closeAfterCounting = true;
            }
            RandomAccessFile readFrom = read(); // this outside of the try so it doesn't effect the state if we are making a copy
            if (readFrom == null) {
                return 0;
            }
            try {
                long fp = readFrom.getFilePointer();
                readFrom.seek(0);
                while (true) {
                    long entryFp = readFrom.getFilePointer();
                    int r = readFrom.read(modeLengthAndTimestamp);
                    if (r != headerSize || r == -1) {
                        break;
                    }
                    long[] mlt = FilerIO.bytesLongs(modeLengthAndTimestamp);
                    readFrom.seek(readFrom.getFilePointer() + mlt[1] + endOfDataMarkerSize);
                    if (mlt[0] == REMOVED) {
                        continue;
                    }
                    if (mlt[0] != 0) {
                        setMode(entryFp, makeItThisMode);
                    }
                    bruteForceCount++;
                }
                readFrom.seek(fp); // put fp back to where it was
                if (closeAfterCounting) {
                    close();
                }

                if (bruteForceCount == 0 && removeIfCountIsZero) {
                    logger.debug("Deleting " + file + " because bruteForceCount is zero");
                    delete();
                }
            } catch (IOException ioex) {
                throw new RuntimeException("Failed to count file=" + file + " openFilesCount=" + openFilesCount, ioex);
            }
        }
        return bruteForceCount;
    }

    @Override
    public int hashCode() {
        return file.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final FileQueue other = (FileQueue) obj;
        if (this.file != other.file && (this.file == null || !this.file.equals(other.file))) {
            return false;
        }
        return true;
    }

    private RandomAccessFile write() {
        synchronized (ioLock) {
            if (state == State.OFF || state == State.READING) {
                if (write != null) {
                    return write;
                }
                try {
                    state = State.OPENING_READING;
                    ensureFileExists();
                    write = new RandomAccessFile(file, "rw"); // open for appending
                    openFilesCount++;
                    state = State.READING;
                    return write;
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("couldnt create writer FileNotFoundException " + e.getMessage(), e);
                } catch (IOException e) {
                    throw new RuntimeException("couldnt create writer " + e.getMessage() + " openFilesCount=" + openFilesCount, e);
                }
            } else {
                throw new RuntimeException("Queue is in a bad state=" + state + " expected " + State.OFF + " or " + State.READING);
            }
        }
    }

    private RandomAccessFile read() {
        synchronized (ioLock) {
            if (state == State.OFF || state == State.WRITING) {
                if (read != null) {
                    return read;
                }
                try {
                    state = State.OPENING_WRITING;
                    ensureFileExists();
                    read = new RandomAccessFile(file, "rw");
                    openFilesCount++;
                    state = State.WRITING;
                    return read;
                } catch (FileNotFoundException e) {
                    logger.error("FileNotFoundException openFilesCount=" + openFilesCount, e);
                    return null;
                } catch (IOException e) {
                    logger.error("IOException openFilesCount=" + openFilesCount, e);
                    return null;
                }
            } else {
                logger.error("Queue is in a bad state=" + state + " expected " + State.OFF + " or " + State.WRITING);
                return null;
            }
        }
    }

    public void close() {
        synchronized (ioLock) {
            if (state == State.FAILED_TO_DELETE || state == State.DELETED) {
                logger.warn("WARNING you cannot close a file that was deleted!");
                return;
            }
            try {
                if (read != null) {
                    read.close();
                    openFilesCount--;
                }
                read = null;

                if (write != null) {
                    write.getFD().sync(); // this is hacky but hey we want this to happen a soon as possible
                    write.close();
                    openFilesCount--;
                }
                write = null;
                state = State.OFF;

            } catch (IOException ex) {
                state = State.OFF;
                throw new RuntimeException("Failed to close file=" + file, ex);
            }
        }
    }

    public boolean delete() {
        synchronized (ioLock) {
            close();
            if (!file.exists()) {
                state = State.DELETED;
                return true;
            } else if (file.delete()) {
                logger.debug("Deleted " + file);
                state = State.DELETED;
                return true;
            } else {
                state = State.FAILED_TO_DELETE;
                throw new RuntimeException("Tried to delete but failed for file=" + file);
            }

        }
    }
}
