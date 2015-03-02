/*
 * Copyright 2015 Jive Software.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.filer.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 *
 * @author jonathan.colt
 */
public class AutoGrowingByteBufferBackedFiler implements Filer {

    public static final long MAX_BUFFER_SEGMENT_SIZE = FilerIO.chunkLength(30);
    public static long MAX_POSITION = MAX_BUFFER_SEGMENT_SIZE * 100;

    private final ByteBufferFactory byteBufferFactory;
    private final long initialBufferSegmentSize;
    private final long maxBufferSegmentSize;

    private ByteBufferBackedFiler[] filers;
    private int fpFilerIndex;

    public AutoGrowingByteBufferBackedFiler(ByteBufferFactory byteBufferFactory,
        long initialBufferSegmentSize,
        long maxBufferSegmentSize) throws IOException {
        this.byteBufferFactory = byteBufferFactory;
        this.initialBufferSegmentSize = FilerIO.chunkLength(FilerIO.chunkPower(initialBufferSegmentSize, 0));
        this.maxBufferSegmentSize = Math.min(FilerIO.chunkLength(FilerIO.chunkPower(maxBufferSegmentSize, 0)), MAX_BUFFER_SEGMENT_SIZE);
        this.filers = new ByteBufferBackedFiler[0];
    }

    private AutoGrowingByteBufferBackedFiler(long maxBufferSegmentSize, ByteBufferBackedFiler[] filers) {
        this.byteBufferFactory = null;
        this.initialBufferSegmentSize = -1;
        this.maxBufferSegmentSize = maxBufferSegmentSize;
        this.filers = filers;
        this.fpFilerIndex = -1;
    }

    public AutoGrowingByteBufferBackedFiler duplicate(long startFP, long endFp) {
        ByteBufferBackedFiler[] duplicate = new ByteBufferBackedFiler[filers.length];
        for (int i = 0; i < duplicate.length; i++) {
            if ((i + 1) * maxBufferSegmentSize < startFP || (i - 1) * maxBufferSegmentSize > endFp) {
                continue;
            }
            duplicate[i] = new ByteBufferBackedFiler(filers[i].buffer.duplicate());
        }
        return new AutoGrowingByteBufferBackedFiler(maxBufferSegmentSize, duplicate);
    }

    public AutoGrowingByteBufferBackedFiler duplicateNew(AutoGrowingByteBufferBackedFiler current) {
        ByteBufferBackedFiler[] duplicate = new ByteBufferBackedFiler[filers.length];
        System.arraycopy(current.filers, 0, duplicate, 0, current.filers.length - 1);
        for (int i = current.filers.length - 1; i < duplicate.length; i++) {
            duplicate[i] = new ByteBufferBackedFiler(filers[i].buffer.duplicate());
        }
        return new AutoGrowingByteBufferBackedFiler(maxBufferSegmentSize, duplicate);
    }

    public AutoGrowingByteBufferBackedFiler duplicateAll() {
        ByteBufferBackedFiler[] duplicate = new ByteBufferBackedFiler[filers.length];
        for (int i = 0; i < duplicate.length; i++) {
            duplicate[i] = new ByteBufferBackedFiler(filers[i].buffer.duplicate());
        }
        return new AutoGrowingByteBufferBackedFiler(maxBufferSegmentSize, duplicate);
    }

    public boolean exists() {
        byte[] key = String.valueOf(0)
            .getBytes(StandardCharsets.UTF_8);
        return byteBufferFactory.exists(key);
    }

    final void position(long position) throws IOException {
        if (position > MAX_POSITION) {
            throw new IllegalStateException("Encountered a likely runaway file position! position=" + position);
        }
        int f = (int) (position / maxBufferSegmentSize); // TODO can/should convert to bitshifts?
        long fseek = position % maxBufferSegmentSize; // TODO can/should  convert to bitshifts?
        if (f >= filers.length) {
            int lastFilerIndex = filers.length - 1;
            if (lastFilerIndex > -1 && filers[lastFilerIndex].length() < maxBufferSegmentSize) {
                byte[] key = String.valueOf(lastFilerIndex)
                    .getBytes(StandardCharsets.UTF_8);
                ByteBuffer reallocate = byteBufferFactory.reallocate(key, filers[lastFilerIndex].buffer, maxBufferSegmentSize);
                filers[lastFilerIndex] = new ByteBufferBackedFiler(reallocate);
            }

            int newLength = f + 1;
            ByteBufferBackedFiler[] newFilers = new ByteBufferBackedFiler[newLength];
            System.arraycopy(filers, 0, newFilers, 0, filers.length);
            for (int n = filers.length; n < newLength; n++) {
                byte[] key = String.valueOf(n)
                    .getBytes(StandardCharsets.UTF_8);
                if (n < newLength - 1) {
                    newFilers[n] = new ByteBufferBackedFiler(byteBufferFactory.allocate(key, maxBufferSegmentSize));
                } else {
                    newFilers[n] = new ByteBufferBackedFiler(byteBufferFactory.allocate(key, Math.max(fseek, initialBufferSegmentSize)));
                }
            }
            filers = newFilers;

        } else if (f == filers.length - 1 && fseek > filers[f].length()) {
            long newSize = filers[f].length() * 2;
            while (newSize < fseek) {
                newSize *= 2;
            }
            byte[] key = String.valueOf(f)
                .getBytes(StandardCharsets.UTF_8);
            ByteBuffer reallocate = byteBufferFactory.reallocate(key, filers[f].buffer, Math.min(maxBufferSegmentSize, newSize));
            filers[f] = new ByteBufferBackedFiler(reallocate);
        }
        filers[f].seek(fseek);
        fpFilerIndex = f;
    }

    @Override
    public final void seek(long position) throws IOException {
        position(position);
    }

    @Override
    public long skip(long skip) throws IOException {
        long fp = getFilePointer();
        position(fp + skip);
        return skip;
    }

    @Override
    public long length() throws IOException {
        if (filers.length == 0) {
            return 0;
        }
        return ((filers.length - 1) * maxBufferSegmentSize) + filers[filers.length - 1].length();
    }

    @Override
    public void setLength(long len) throws IOException {
        position(len);
    }

    @Override
    public long getFilePointer() throws IOException {
        if (filers.length == 0) {
            return 0;
        }
        long fp = (fpFilerIndex * maxBufferSegmentSize) + filers[fpFilerIndex].getFilePointer();
        return fp;
    }

    @Override
    public void eof() throws IOException {
        position(length());
    }

    @Override
    public void flush() throws IOException {
        for (ByteBufferBackedFiler filer : filers) {
            filer.flush();
        }
    }

    @Override
    public int read() throws IOException {
        int read = filers[fpFilerIndex].read();
        while (read == -1 && fpFilerIndex < filers.length - 1) {
            fpFilerIndex++;
            filers[fpFilerIndex].seek(0);
            read = filers[fpFilerIndex].read();
        }
        return read;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int offset, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int remaining = len;
        int read = filers[fpFilerIndex].read(b, offset, remaining);
        if (read == -1) {
            read = 0;
        }
        offset += read;
        remaining -= read;
        while (remaining > 0 && fpFilerIndex < filers.length - 1) {
            fpFilerIndex++;
            filers[fpFilerIndex].seek(0);
            read = filers[fpFilerIndex].read(b, offset, remaining);
            if (read == -1) {
                read = 0;
            }
            offset += read;
            remaining -= read;
        }
        if (len == remaining) {
            return -1;
        }
        return offset;
    }

    @Override
    public void write(int b) throws IOException {
        if (filers.length == 0 || filers[fpFilerIndex].getFilePointer() == filers[fpFilerIndex].length()) {
            position(getFilePointer());
        }
        filers[fpFilerIndex].write(b);

    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int offset, int len) throws IOException {
        long fp = getFilePointer();
        position(fp + len);
        position(fp);

        long canWrite = Math.min(len, filers[fpFilerIndex].length() - filers[fpFilerIndex].getFilePointer());
        filers[fpFilerIndex].write(b, offset, (int) canWrite);
        long remaingToWrite = len - canWrite;
        offset += canWrite;
        while (remaingToWrite > 0) {
            fpFilerIndex++;
            filers[fpFilerIndex].seek(0);
            canWrite = Math.min(remaingToWrite, filers[fpFilerIndex].length() - filers[fpFilerIndex].getFilePointer());
            filers[fpFilerIndex].write(b, offset, (int) canWrite);
            remaingToWrite -= canWrite;
            offset += canWrite;
        }
    }

    @Override
    public void close() throws IOException {
        for (ByteBufferBackedFiler filer : filers) {
            filer.close();
        }
    }

}
