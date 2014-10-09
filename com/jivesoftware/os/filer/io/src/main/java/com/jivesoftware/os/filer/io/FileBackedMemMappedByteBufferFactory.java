package com.jivesoftware.os.filer.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author jonathan.colt
 */
public class FileBackedMemMappedByteBufferFactory implements ByteBufferFactory {

    public static final int DEFAULT_64BIT_MAX_BUFF = (1 << 30);
    private final long maxSizeForByteBuffer;

    private final File file;

    public FileBackedMemMappedByteBufferFactory(File file) {
        this(file, DEFAULT_64BIT_MAX_BUFF);
    }

    public FileBackedMemMappedByteBufferFactory(File file, long maxSizeForByteBuffer) {
        this.file = file;
        this.maxSizeForByteBuffer = maxSizeForByteBuffer;
    }

    public MappedByteBuffer open() {
        try {
            ensureDirectory(file);
            MappedByteBuffer buf;
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.seek(0);
                FileChannel channel = raf.getChannel();
                buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, (int) channel.size());
            }
            return buf;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer allocate(long length) {

        ensureDirectory(file);
        MappedByteBuffer buf;
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(length);
            raf.write(0);
            raf.seek(0);
            FileChannel channel = raf.getChannel();
            buf = channel.map(FileChannel.MapMode.READ_WRITE, 0, (int) channel.size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buf;
    }

    // this was borrowed from lucene ByteBufferIndexInput
    ByteBuffer[] map(RandomAccessFile raf, long offset, long length) throws IOException {
        if ((length >>> maxSizeForByteBuffer) >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("RandomAccessFile too big for chunk size: " + raf.toString());
        }

        final long chunkSize = 1L << maxSizeForByteBuffer;

        // we always allocate one more buffer, the last one may be a 0 byte one
        final int nrBuffers = (int) (length >>> maxSizeForByteBuffer) + 1;

        ByteBuffer buffers[] = new ByteBuffer[nrBuffers];

        long bufferStart = 0L;
        FileChannel rafc = raf.getChannel();
        for (int bufNr = 0; bufNr < nrBuffers; bufNr++) {
            int bufSize = (int) ((length > (bufferStart + chunkSize))
                    ? chunkSize
                    : (length - bufferStart));
            buffers[bufNr] = rafc.map(FileChannel.MapMode.READ_ONLY, offset + bufferStart, bufSize);
            bufferStart += bufSize;
        }

        return buffers;
    }

    private void ensureDirectory(File _file) {
        if (!_file.exists()) {
            File parent = _file.getParentFile();
            if (parent != null && !parent.mkdirs()) {
                if (!parent.exists()) {
                    throw new RuntimeException("Failed to create parent:" + parent);
                }
            }
        }
    }

}
