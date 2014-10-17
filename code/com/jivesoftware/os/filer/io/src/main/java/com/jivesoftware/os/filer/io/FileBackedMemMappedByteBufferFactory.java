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

    private final File file;

    public FileBackedMemMappedByteBufferFactory(File file) {
        this.file = file;
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

    @Override
    public ByteBuffer reallocate(ByteBuffer oldBuffer, long newSize) {
        return allocate(newSize);
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
