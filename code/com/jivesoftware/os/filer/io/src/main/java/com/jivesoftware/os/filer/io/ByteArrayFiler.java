/*
 * Copyright 2014 Jive Software.
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

/**
 *
 * @author jonathan
 */
public class ByteArrayFiler implements Filer {

    private byte[] bytes = new byte[0];
    private long fp = 0;

    public ByteArrayFiler() {
    }

    public ByteArrayFiler(byte[] _bytes) {
        bytes = _bytes;
    }

    public byte[] getBytes() {
        return trim(bytes, (int) fp);
    }

    public byte[] leakBytes() {
        return bytes;
    }

    public void reset() {
        fp = 0;
        bytes = new byte[0];
    }

    public Object lock() {
        return this;
    }

    public int read() throws IOException {
        if (fp >= bytes.length) {
            return -1;
        }
        int b = (int) (bytes[(int) fp] & 0xFF);
        fp++;
        return b;
    }

    public int read(byte[] b) throws IOException {
        if (b == null) {
            return 0;
        }
        if (fp > bytes.length) {
            return -1;
        }
        int len = b.length;
        if (bytes.length - (int) fp < len) {
            len = bytes.length - (int) fp;
        }
        if (len == 0) {
            return -1;
        }
        System.arraycopy(bytes, (int) fp, b, 0, len);
        fp += len;
        return len;
    }

    public int read(byte b[], int _offset, int _len) throws IOException {
        if (b == null) {
            return 0;
        }
        if (fp > bytes.length) {
            return -1;
        }
        int len = _len;
        if (fp + len > bytes.length) {
            len = (int) (bytes.length - fp);
        }
        System.arraycopy(bytes, (int) fp, b, _offset, len);
        fp += len;
        return len;
    }

    public void write(int b) throws IOException {
        if (fp + 1 > bytes.length) {
            bytes = grow(bytes, 1 + (bytes.length * 2));
        }
        bytes[(int) fp] = (byte) b;
        fp++;
    }

    public void write(byte _b[]) throws IOException {
        if (_b == null) {
            return;
        }
        int len = _b.length;
        if (fp + len > bytes.length) {
            bytes = grow(bytes, len + (bytes.length * 2));
        }
        System.arraycopy(_b, 0, bytes, (int) fp, len);
        fp += len;
    }

    public void write(byte _b[], int _offset, int _len) throws IOException {
        if (_b == null) {
            return;
        }
        int len = _len;
        if (fp + len > bytes.length) {
            bytes = grow(bytes, len + (bytes.length * 2));
        }
        System.arraycopy(_b, _offset, bytes, (int) fp, len);
        fp += len;
    }

    public long getFilePointer() throws IOException {
        return fp;
    }

    public void seek(long _position) throws IOException {
        fp = _position;
    }

    public long skip(long _position) throws IOException {
        fp += _position;
        return fp;
    }

    public void setLength(long len) throws IOException {
        if (len < 0) {
            throw new IOException();
        }
        byte[] newBytes = new byte[(int) len];
        System.arraycopy(bytes, 0, newBytes, 0, Math.min(bytes.length, newBytes.length));
        fp = (int) len;
        bytes = newBytes;
    }

    public long length() throws IOException {
        return bytes.length;
    }

    public void eof() throws IOException {
        bytes = trim(bytes, (int) fp);
    }

    public void close() throws IOException {
    }

    public void flush() throws IOException {
    }

    static final private byte[] trim(byte[] src, int count) {
        byte[] newSrc = new byte[count];
        System.arraycopy(src, 0, newSrc, 0, count);
        return newSrc;
    }

    static final public byte[] grow(byte[] src, int amount) {
        if (src == null) {
            return new byte[amount];
        }
        byte[] newSrc = new byte[src.length + amount];
        System.arraycopy(src, 0, newSrc, 0, src.length);
        return newSrc;
    }
}
