package com.jivesoftware.os.filer.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jonathan.colt
 */
public class RandomAccessFiler extends RandomAccessFile implements ConcurrentFiler {

    public static long totalFilesOpenCount;
    public static long totalReadByteCount;
    public static long totalWriteByteCount;
    public static long totalSeeksCount;
    public long readByteCount;
    public long writeByteCount;
    String fileName;

    public RandomAccessFiler(String name, String mode) throws IOException {
        super(name, mode);
        fileName = name;
    }

    public RandomAccessFiler(File file, String mode) throws IOException {
        super(file, mode);
        fileName = file.getName();
    }

    @Override
    public String toString() {
        try {
            return "R:" + (readByteCount / 1024) + "kb W:" + (writeByteCount / 1024) + "kb " + fileName + " " + (length() / 1024) + "kb";
        } catch (IOException x) {
            return "R:" + (readByteCount / 1024) + "kb W:" + (writeByteCount / 1024) + "kb " + fileName;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public Object lock() {
        return this;
    }

    @Override
    public long skip(long position) throws IOException {
        long fp = getFilePointer();
        if (position == 0) {
            return fp;
        } else if (position < 0) {
            if (-position > fp) {
                throw new IOException("Skipped off the end of the beginning");
            }
            seek(fp + position);
        } else {
            if (Long.MAX_VALUE - fp > position) {
                seek(fp + position);
            } else {
                throw new IOException("Skipped off the end of the World");
            }
        }
        return getFilePointer();
    }

    @Override
    public void seek(long _fp) throws IOException {
        totalSeeksCount++;
        super.seek(_fp);
    }

    @Override
    public int read() throws IOException {
        readByteCount++;
        totalReadByteCount++;
        return super.read();
    }

    @Override
    public int read(byte b[]) throws IOException {
        int off = 0;
        int len = b.length;
        int n = 0;
        while (n < len) {
            int count = super.read(b, off + n, len - n);
            if (count < 0) {
                len = n;
                if (n == 0) {
                    len = -1;
                }
                break;
            }
            n += count;
        }
        readByteCount += len;
        totalReadByteCount += len;
        return len;
    }

    @Override
    public int read(byte b[], int _offset, int _len) throws IOException {
        int len = super.read(b, _offset, _len);
        readByteCount += len;
        totalReadByteCount += len;
        return len;
    }

    @Override
    public void write(int b) throws IOException {
        writeByteCount++;
        totalWriteByteCount++;
        super.write(b);
    }

    @Override
    public void write(byte b[]) throws IOException {
        if (b != null) {
            writeByteCount += b.length;
            totalWriteByteCount += b.length;
        }
        super.write(b);
    }

    @Override
    public void write(byte b[], int _offset, int _len) throws IOException {
        super.write(b, _offset, _len);
        writeByteCount += _len;
        totalWriteByteCount += _len;
    }

    @Override
    public void eof() throws IOException {
        setLength(getFilePointer());
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public ConcurrentFiler asConcurrentReadWrite(Object suggestedLock) throws IOException {
        return this;
    }

    @Override
    public void delete() throws IOException {
        FileUtils.forceDelete(new File(fileName));
    }

    @Override
    public long capacity() {
        return Long.MAX_VALUE;
    }
}
