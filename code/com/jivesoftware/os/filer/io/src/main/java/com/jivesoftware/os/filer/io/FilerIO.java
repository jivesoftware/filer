package com.jivesoftware.os.filer.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 *
 * @author Administrator
 */
public class FilerIO {

    /**
     *
     * @param _from
     * @param _to
     * @param _bufferSize
     * @return
     * @throws Exception
     */
    public static long copy(Readable _from, Writeable _to, long _bufferSize) throws IOException {
        long byteCount = _bufferSize;
        if (_bufferSize < 1) {
            byteCount = 1024 * 1024; //1MB
        }
        byte[] chunk = new byte[(int) byteCount];
        int bytesRead;
        long size = 0;
        while ((bytesRead = _from.read(chunk)) > -1) {
            _to.write(chunk, 0, bytesRead);
            size += bytesRead;
        }
        return size;
    }

    /**
     *
     * @param _from
     * @param _to
     * @param _bufferSize
     * @return
     * @throws Exception
     */
    public static long copy(Readable _from, Writeable _to, long _maxBytes, long _bufferSize) throws IOException {
        long byteCount = _bufferSize;
        if (_bufferSize < 1) {
            byteCount = 1024 * 1024; //1MB
        }
        byteCount = Math.min(byteCount, _maxBytes);

        byte[] chunk = new byte[(int) byteCount];
        int remaining = (int) _maxBytes;
        int bytesRead;
        long size = 0;
        while (remaining > 0 && (bytesRead = _from.read(chunk, 0, (int) Math.min(remaining, byteCount))) > -1) {
            _to.write(chunk, 0, bytesRead);
            size += bytesRead;
            remaining -= bytesRead;
        }
        return size;
    }

    /**
     *
     * @param _in
     * @param _bufferSize
     * @return
     * @throws IOException
     */
    public static byte[] toByteArray(InputStream _in, int _bufferSize) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[_bufferSize];
        int bytesRead;
        while ((bytesRead = _in.read(buffer)) > -1) {
            out.write(buffer, 0, bytesRead);
        }
        _in.close();
        return out.toByteArray();
    }

    /**
     *
     * @param _ints
     * @return
     */
    public static byte[] intArrayToByteArray(int[] _ints) {
        int len = _ints.length;
        byte[] bytes = new byte[len * 4]; //peformance hack
        int index = 0;
        for (int i = 0; i < len; i++) {
            int v = _ints[i];
            bytes[index++] = (byte) (v >>> 24);
            bytes[index++] = (byte) (v >>> 16);
            bytes[index++] = (byte) (v >>> 8);
            bytes[index++] = (byte) v;
        }
        return bytes;
    }

    // Writing
    /**
     *
     * @param _filer
     * @param _line
     * @throws IOException
     */
    public static void writeLine(Writeable _filer, String _line) throws IOException {
        _filer.write((_line + "\n").getBytes());
    }

    /**
     *
     * @param _filer
     * @param b
     * @param fieldName
     * @throws IOException
     */
    public static void write(Writeable _filer, int b, String fieldName) throws IOException {
        _filer.write(b);
    }

    /**
     *
     * @param _filer
     * @param bytes
     * @param fieldName
     * @throws IOException
     */
    public static void write(Writeable _filer, byte[] bytes,
        String fieldName) throws IOException {
        _filer.write(bytes);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeBoolean(Writeable _filer, boolean v,
        String fieldName) throws IOException {
        _filer.write(v ? 1 : 0);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeByte(Writeable _filer, int v,
        String fieldName) throws IOException {
        _filer.write(v);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeShort(Writeable _filer, int v,
        String fieldName) throws IOException {
        _filer.write(new byte[]{
            (byte) (v >>> 8),
            (byte) v
        });
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeChar(Writeable _filer, int v,
        String fieldName) throws IOException {
        _filer.write(new byte[]{
            (byte) (v >>> 8),
            (byte) v
        });
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeInt(Writeable _filer, int v, String fieldName) throws IOException {
        _filer.write(new byte[]{
            (byte) (v >>> 24),
            (byte) (v >>> 16),
            (byte) (v >>> 8),
            (byte) v
        });
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeLong(Writeable _filer, long v,
        String fieldName) throws IOException {
        _filer.write(new byte[]{
            (byte) (v >>> 56),
            (byte) (v >>> 48),
            (byte) (v >>> 40),
            (byte) (v >>> 32),
            (byte) (v >>> 24),
            (byte) (v >>> 16),
            (byte) (v >>> 8),
            (byte) v
        });
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] intsToBytes(int[] v) {
        byte[] result = new byte[4 * v.length];
        int j;
        for (int i = 0; i < v.length; i++) {
            j = i * 4;
            result[j + 0] = (byte) (v[i] >>> 24);
            result[j + 1] = (byte) (v[i] >>> 16);
            result[j + 2] = (byte) (v[i] >>> 8);
            result[j + 3] = (byte) v[i];
        }
        return result;
    }

    /**
     *
     * @param v
     * @return
     */
    public static int[] bytesToInts(byte[] v) {
        int[] result = new int[(v.length + 3) / 4];
        int j;
        for (int i = 0; i < result.length; i++) {
            j = i * 4;
            int k = 0;
            k |= (v[j + 0] & 0xFF);
            k <<= 8;
            k |= (v[j + 1] & 0xFF);
            k <<= 8;
            k |= (v[j + 2] & 0xFF);
            k <<= 8;
            k |= (v[j + 3] & 0xFF);
            result[i] = k;
        }
        return result;
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] charBytes(char v) {
        return new byte[]{
            (byte) (v >>> 8),
            (byte) v
        };
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeFloat(Writeable _filer, float v,
        String fieldName) throws IOException {
        writeInt(_filer, Float.floatToIntBits(v), fieldName);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeDouble(Writeable _filer, double v,
        String fieldName) throws IOException {
        writeLong(_filer, Double.doubleToLongBits(v), fieldName);
    }

    /**
     *
     * @param _filer
     * @param l
     * @throws IOException
     */
    public static void writeLength(Writeable _filer, int l) throws IOException {
        writeInt(_filer, l, "length");
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeBooleanArray(Writeable _filer,
        boolean[] array, String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            _filer.write(array[i] ? 1 : 0);
        }
    }

    /**
     *
     * @param _filer
     * @param array
     * @throws IOException
     */
    public static void write(Writeable _filer, byte[] array) throws IOException {
        _filer.write(array);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeByteArray(Writeable _filer, byte[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        _filer.write(array);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param _start
     * @param _len
     * @param fieldName
     * @throws IOException
     */
    public static void writeByteArray(Writeable _filer, byte[] array,
        int _start, int _len, String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = _len;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        _filer.write(array, _start, len);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeCharArray(Writeable _filer, char[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        int index = 0;
        byte[] buffer = new byte[len * 2]; //peformance hack
        for (int i = 0; i < len; i++) {
            int v = array[i];
            buffer[index++] = (byte) (v >>> 8);
            buffer[index++] = (byte) v;
        }
        _filer.write(buffer);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeShortArray(Writeable _filer, short[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        int index = 0;
        byte[] buffer = new byte[len * 2]; //peformance hack
        for (int i = 0; i < len; i++) {
            int v = array[i];
            buffer[index++] = (byte) (v >>> 8);
            buffer[index++] = (byte) v;
        }
        _filer.write(buffer);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeIntArray(Writeable _filer, int[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        int index = 0;
        byte[] buffer = new byte[len * 4]; //peformance hack
        for (int i = 0; i < len; i++) {
            int v = array[i];
            buffer[index++] = (byte) (v >>> 24);
            buffer[index++] = (byte) (v >>> 16);
            buffer[index++] = (byte) (v >>> 8);
            buffer[index++] = (byte) v;
        }
        _filer.write(buffer);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeStringArray(Writeable _filer, Object[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            String s = null;
            if (array[i] instanceof String) {
                s = (String) array[i];
            } else if (array[i] != null) {
                s = array[i].toString();
            }
            if (s == null) {
                s = "";
            }
            writeCharArray(_filer, s.toCharArray(), fieldName);
        }
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeLongArray(Writeable _filer, long[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            long v = array[i];
            _filer.write(new byte[]{
                (byte) (v >>> 56),
                (byte) (v >>> 48),
                (byte) (v >>> 40),
                (byte) (v >>> 32),
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
            });
        }
    }

    /**
     *
     * @param _filer
     * @param array
     * @param _start
     * @param _len
     * @param fieldName
     * @throws IOException
     */
    public static void writeLongArray(Writeable _filer, long[] array,
        int _start, int _len, String fieldName) throws IOException {
        writeLength(_filer, _len);
        if (_len < 0) {
            return;
        }
        for (int i = _start; i < _start + _len; i++) {
            long v = array[i];
            _filer.write(new byte[]{
                (byte) (v >>> 56),
                (byte) (v >>> 48),
                (byte) (v >>> 40),
                (byte) (v >>> 32),
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
            });
        }
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeFloatArray(Writeable _filer, float[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            int v = Float.floatToIntBits(array[i]);
            _filer.write(new byte[]{
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
            });
        }
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeDoubleArray(Writeable _filer, double[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            long v = Double.doubleToLongBits(array[i]);
            _filer.write(new byte[]{
                (byte) (v >>> 56),
                (byte) (v >>> 48),
                (byte) (v >>> 40),
                (byte) (v >>> 32),
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
            });
        }
    }

    /**
     *
     * @param _filer
     * @param s
     * @param fieldName
     * @throws IOException
     */
    public static void writeString(Writeable _filer, String s,
        String fieldName) throws IOException {
        if (s == null) {
            s = "";
        }
        writeCharArray(_filer, s.toCharArray(), fieldName);
    }

    /**
     *
     * @param _filer
     * @param _tag
     * @throws Exception
     */
    public static void readBegin(Readable _filer, String _tag) throws Exception {
    }

    /**
     *
     * @param _filer
     * @param _tag
     * @throws Exception
     */
    public static void readEnd(Readable _filer, String _tag) throws Exception {
    }

    /**
     *
     * @param _filer
     * @return
     * @throws IOException
     */
    public static int readLength(Readable _filer) throws IOException {
        return readInt(_filer, "length");
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static boolean[] readBooleanArray(Readable _filer,
        String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new boolean[0];
        }
        boolean[] array = new boolean[len];
        byte[] bytes = new byte[len];
        _filer.read(bytes);
        for (int i = 0; i < len; i++) {
            array[i] = (bytes[i] != 0);
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param array
     * @throws IOException
     */
    public static void read(Readable _filer, byte[] array) throws IOException {
        _filer.read(array);
    }

    /**
     *
     * @param _filer
     * @param array
     * @throws IOException
     */
    public static void read(Readable _filer, byte[] array, int offset, int length) throws IOException {
        _filer.read(array, offset, length);
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static byte[] readByteArray(Readable _filer, String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        byte[] array = new byte[len];
        _filer.read(array);
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static char[] readCharArray(Readable _filer, String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new char[0];
        }
        char[] array = new char[len];
        byte[] bytes = new byte[2 * len];
        _filer.read(bytes);
        int j;
        for (int i = 0; i < len; i++) {
            j = i * 2;
            char v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            array[i] = v;
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static short[] readShortArray(Readable _filer,
        String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new short[0];
        }
        short[] array = new short[len];
        byte[] bytes = new byte[2 * len];
        _filer.read(bytes);
        int j;
        for (int i = 0; i < len; i++) {
            j = i * 2;
            short v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            array[i] = v;
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static int[] readIntArray(Readable _filer, String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new int[0];
        }
        int[] array = new int[len];
        byte[] bytes = new byte[4 * len];
        _filer.read(bytes);
        int j;
        for (int i = 0; i < len; i++) {
            j = i * 4;
            int v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 2] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 3] & 0xFF);
            array[i] = v;
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static String[] readStringArray(Readable _filer,
        String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new String[0];
        }
        String[] array = new String[len];
        for (int i = 0; i < len; i++) {
            array[i] = readString(_filer, fieldName);
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static long[] readLongArray(Readable _filer, String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new long[0];
        }
        long[] array = new long[len];
        byte[] bytes = new byte[8 * len];
        _filer.read(bytes);
        int j;
        for (int i = 0; i < len; i++) {
            j = i * 8;
            long v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 2] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 3] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 4] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 5] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 6] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 7] & 0xFF);
            array[i] = v;
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static float[] readFloatArray(Readable _filer,
        String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new float[0];
        }
        float[] array = new float[len];
        byte[] bytes = new byte[4 * len];
        _filer.read(bytes);
        int j;
        for (int i = 0; i < len; i++) {
            j = i * 4;
            int v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 2] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 3] & 0xFF);
            array[i] = Float.intBitsToFloat(v);
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static double[] readDoubleArray(Readable _filer,
        String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new double[0];
        }
        double[] array = new double[len];
        byte[] bytes = new byte[8 * len];
        _filer.read(bytes);
        int j;
        for (int i = 0; i < len; i++) {
            j = i * 8;
            long v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 2] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 3] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 4] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 5] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 6] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 7] & 0xFF);
            array[i] = Double.longBitsToDouble(v);
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static String readText(Readable _filer, String fieldName) throws Exception {
        return new String(readCharArray(_filer, fieldName));
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static String readString(Readable _filer, String fieldName) throws IOException {
        return new String(readCharArray(_filer, fieldName));
    }

    // Reading
    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static boolean readBoolean(Readable _filer, String fieldName) throws IOException {
        int v = _filer.read();
        if (v < 0) {
            throw new EOFException();
        }
        return (v != 0);
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static byte readByte(Readable _filer, String fieldName) throws IOException {
        return (byte) _filer.read();
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static int readUnsignedByte(Readable _filer, String fieldName) throws IOException {
        return _filer.read();
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static short readShort(Readable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[2];
        _filer.read(bytes);
        short v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static int readUnsignedShort(Readable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[2];
        _filer.read(bytes);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static char readChar(Readable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[2];
        _filer.read(bytes);
        char v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    /**
     *
     * @param bytes
     * @return
     */
    public static char bytesChar(byte[] bytes) {
        char v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static int readInt(Readable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[4];
        _filer.read(bytes);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static long readLong(Readable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[8];
        _filer.read(bytes);
        long v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        v <<= 8;
        v |= (bytes[4] & 0xFF);
        v <<= 8;
        v |= (bytes[5] & 0xFF);
        v <<= 8;
        v |= (bytes[6] & 0xFF);
        v <<= 8;
        v |= (bytes[7] & 0xFF);

        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static float readFloat(Readable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[4];
        _filer.read(bytes);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        return Float.intBitsToFloat(v);
    }

    /**
     *
     * @param chars
     * @return
     */
    public static byte[] charsBytes(char[] chars) {
        byte[] bytes = new byte[chars.length * 2];
        for (int i = 0; i < chars.length; i++) {
            char v = chars[i];
            bytes[2 * i] = (byte) (v >>> 8);
            bytes[2 * i + 1] = (byte) v;
        }
        return bytes;
    }

    /**
     *
     * @param bytes
     * @return
     */
    public static char[] bytesChars(byte[] bytes) {
        char[] chars = new char[bytes.length / 2];
        for (int i = 0; i < chars.length; i++) {
            char v = 0;
            v |= (bytes[2 * i] & 0xFF);
            v <<= 8;
            v |= (bytes[2 * i + 1] & 0xFF);
            chars[i] = v;
        }
        return chars;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static double readDouble(Readable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[8];
        _filer.read(bytes);
        long v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        v <<= 8;
        v |= (bytes[4] & 0xFF);
        v <<= 8;
        v |= (bytes[5] & 0xFF);
        v <<= 8;
        v |= (bytes[6] & 0xFF);
        v <<= 8;
        v |= (bytes[7] & 0xFF);
        return Double.longBitsToDouble(v);
    }

    /**
     *
     * @param bytes
     * @return
     */
    public static double byteDouble(byte[] bytes) {
        return Double.longBitsToDouble(bytesLong(bytes));
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static String readLine(Filer _filer, String fieldName) throws IOException {
        StringBuilder input = new StringBuilder();
        int c = -1;
        boolean eol = false;

        while (!eol) {
            switch (c = _filer.read()) {
                case -1:
                case '\n':
                    eol = true;
                    break;
                case '\r':
                    eol = true;
                    long cur = _filer.getFilePointer();
                    if ((_filer.read()) != '\n') {
                        _filer.seek(cur);
                    }
                    break;
                default:
                    input.append((char) c);
                    break;
            }
        }

        if ((c == -1) && (input.length() == 0)) {
            return null;
        }
        return input.toString();
    }

    /**
     *
     * @param _bytes
     * @param _offset
     * @param _longs
     * @return
     */
    static public boolean unpackLongs(byte[] _bytes, int _offset, long[] _longs) {
        if (_bytes == null || _bytes.length == 0) {
            return false;
        }
        if (_longs == null || _longs.length == 0) {
            return false;
        }
        int longsCount = _longs.length;
        if (_offset + (longsCount * 8) > _bytes.length) {
            return false;
        }
        for (int i = 0; i < longsCount; i++) {
            _longs[i] = FilerIO.bytesLong(_bytes, _offset + (i * 8));
        }
        return true;
    }

    /**
     *
     * @param _a
     * @return
     */
    static public byte[] packLongs(long _a) {
        return packLongs(new long[]{_a});
    }

    /**
     *
     * @param _a
     * @param _b
     * @return
     */
    static public byte[] packLongs(long _a, long _b) {
        return packLongs(new long[]{_a, _b});
    }

    /**
     *
     * @param _a
     * @param _b
     * @param _c
     * @return
     */
    static public byte[] packLongs(long _a, long _b, long _c) {
        return packLongs(new long[]{_a, _b, _c});
    }

    /**
     *
     * @param _a
     * @param _b
     * @param _c
     * @param _d
     * @return
     */
    static public byte[] packLongs(long _a, long _b, long _c, long _d) {
        return packLongs(new long[]{_a, _b, _c, _d});
    }

    /**
     *
     * @param _a
     * @param _b
     * @param _c
     * @param _d
     * @param _e
     * @return
     */
    static public byte[] packLongs(long _a, long _b, long _c, long _d, long _e) {
        return packLongs(new long[]{_a, _b, _c, _d, _e});
    }

    /**
     *
     * @param _batch
     * @return
     */
    static public byte[] packLongs(long[] _batch) {
        if (_batch == null || _batch.length == 0) {
            return null;
        }
        byte[] bytes = new byte[_batch.length * 8];
        for (int i = 0; i < _batch.length; i++) {
            FilerIO.longBytes(_batch[i], bytes, i * 8);
        }
        return bytes;
    }

    /**
     *
     * @param _bytes
     * @return
     */
    static public long[] unpackLongs(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int longsCount = _bytes.length / 8;
        long[] longs = new long[longsCount];
        for (int i = 0; i < longsCount; i++) {
            longs[i] = FilerIO.bytesLong(_bytes, i * 8);
        }
        return longs;
    }

    /**
     *
     * @param _bytes
     * @param _lengths
     * @return
     */
    public static Object[] split(byte[] _bytes, int[] _lengths) {
        Object[] splits = new Object[_lengths.length];
        int bp = 0;
        for (int i = 0; i < _lengths.length; i++) {
            byte[] split = new byte[_lengths[i]];
            System.arraycopy(_bytes, bp, split, 0, _lengths[i]);
            splits[i] = split;
        }
        return splits;
    }

    /**
     *
     * @param _ints
     * @return
     */
    public static byte[] intsBytes(int[] _ints) {
        int len = _ints.length;
        byte[] bytes = new byte[len * 4];
        for (int i = 0; i < len; i++) {
            intBytes(_ints[i], bytes, i * 4);
        }
        return bytes;
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] intBytes(int v) {
        return intBytes(v, new byte[4], 0);
    }

    /**
     *
     * @param v
     * @param _bytes
     * @param _offset
     * @return
     */
    public static byte[] intBytes(int v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 24);
        _bytes[_offset + 1] = (byte) (v >>> 16);
        _bytes[_offset + 2] = (byte) (v >>> 8);
        _bytes[_offset + 3] = (byte) v;
        return _bytes;
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static int bytesInt(byte[] _bytes) {
        return bytesInt(_bytes, 0);
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static int[] bytesInts(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int intsCount = _bytes.length / 4;
        int[] ints = new int[intsCount];
        for (int i = 0; i < intsCount; i++) {
            ints[i] = bytesInt(_bytes, i * 4);
        }
        return ints;
    }

    /**
     *
     * @param bytes
     * @param _offset
     * @return
     */
    public static int bytesInt(byte[] bytes, int _offset) {
        int v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        return v;
    }

    /**
     *
     * @param _longs
     * @return
     */
    public static byte[] longsBytes(long[] _longs) {
        int len = _longs.length;
        byte[] bytes = new byte[len * 8];
        for (int i = 0; i < len; i++) {
            longBytes(_longs[i], bytes, i * 8);
        }
        return bytes;
    }

    /**
     *
     * @param _v
     * @return
     */
    public static byte[] longBytes(long _v) {
        return longBytes(_v, new byte[8], 0);
    }

    /**
     *
     * @param v
     * @param _bytes
     * @param _offset
     * @return
     */
    public static byte[] longBytes(long v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 56);
        _bytes[_offset + 1] = (byte) (v >>> 48);
        _bytes[_offset + 2] = (byte) (v >>> 40);
        _bytes[_offset + 3] = (byte) (v >>> 32);
        _bytes[_offset + 4] = (byte) (v >>> 24);
        _bytes[_offset + 5] = (byte) (v >>> 16);
        _bytes[_offset + 6] = (byte) (v >>> 8);
        _bytes[_offset + 7] = (byte) v;
        return _bytes;
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static long bytesLong(byte[] _bytes) {
        return bytesLong(_bytes, 0);
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static long[] bytesLongs(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int longsCount = _bytes.length / 8;
        long[] longs = new long[longsCount];
        for (int i = 0; i < longsCount; i++) {
            longs[i] = bytesLong(_bytes, i * 8);
        }
        return longs;
    }

    /**
     *
     * @param bytes
     * @param _offset
     * @return
     */
    public static long bytesLong(byte[] bytes, int _offset) {
        if (bytes == null) {
            return 0;
        }
        long v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 4] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 5] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 6] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 7] & 0xFF);
        return v;
    }

    /**
     *
     * @param _floats
     * @return
     */
    public static byte[] floatsBytes(float[] _floats) {
        int len = _floats.length;
        byte[] bytes = new byte[len * 4]; //peformance hack
        for (int i = 0; i < len; i++) {
            intBytes(Float.floatToIntBits(_floats[i]), bytes, i * 4);
        }
        return bytes;
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] floatBytes(float v) {
        return intBytes(Float.floatToIntBits(v));
    }

    /**
     *
     * @param bytes
     * @return
     */
    public static float bytesFloat(byte[] bytes) {
        return Float.intBitsToFloat(bytesInt(bytes));
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static float[] bytesFloats(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int floatsCount = _bytes.length / 4;
        float[] floats = new float[floatsCount];
        for (int i = 0; i < floatsCount; i++) {
            floats[i] = bytesFloat(_bytes, i * 4);
        }
        return floats;
    }

    /**
     *
     * @param _bytes
     * @param _offset
     * @return
     */
    public static float bytesFloat(byte[] _bytes, int _offset) {
        return Float.intBitsToFloat(bytesInt(_bytes, _offset));
    }

    /**
     *
     * @param _batch
     * @return
     */
    static public byte[] doublesBytes(double[] _batch) {
        long[] batch = new long[_batch.length];
        for (int i = 0; i < batch.length; i++) {
            batch[i] = Double.doubleToLongBits(_batch[i]);
        }
        return packLongs(batch);
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] doubleBytes(double v) {
        return longBytes(Double.doubleToLongBits(v));
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static double bytesDouble(byte[] _bytes) {
        return Double.longBitsToDouble(bytesLong(_bytes));
    }

    /**
     *
     * @param _bytes
     * @return
     */
    static public double[] bytesDoubles(byte[] _bytes) {
        long[] _batch = unpackLongs(_bytes);
        double[] batch = new double[_batch.length];
        for (int i = 0; i < batch.length; i++) {
            batch[i] = Double.longBitsToDouble(_batch[i]);
        }
        return batch;
    }

    /**
     *
     * @param _bytes
     * @param _offset
     * @return
     */
    public static double bytesDouble(byte[] _bytes, int _offset) {
        return Double.longBitsToDouble(bytesLong(_bytes, _offset));
    }

    /**
     *
     * @param length
     * @param _minPower
     * @return
     */
    public static int chunkPower(long length, int _minPower) {
        int numberOfTrailingZeros = Long.numberOfLeadingZeros(length - 1);
        return Math.max(_minPower, 64 - numberOfTrailingZeros);
    }

    /**
     *
     * @param _chunkPower
     * @return
     */
    public static long chunkLength(int _chunkPower) {
        return 1L << _chunkPower;
    }


    /**
     * Returns a {@link DataInput} which delegates to the given {@link Filer}.
     *
     * @param _filer the delegated filer
     * @return a delegating input
     */
    public static DataInput asDataInput(final Filer _filer) {
        return new DataInput() {

            @Override
            public void readFully(byte[] b) throws IOException {
                int bytesRead = _filer.read(b);
                if (bytesRead < b.length) {
                    throw new EOFException();
                }
            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {
                int bytesRead = _filer.read(b, off, len);
                if (bytesRead < len) {
                    throw new EOFException();
                }
            }

            @Override
            public int skipBytes(int n) throws IOException {
                long fp = _filer.getFilePointer();
                return (int) (_filer.skip(n) - fp);
            }

            @Override
            public boolean readBoolean() throws IOException {
                return FilerIO.readBoolean(_filer, null);
            }

            @Override
            public byte readByte() throws IOException {
                return FilerIO.readByte(_filer, null);
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return FilerIO.readUnsignedByte(_filer, null);
            }

            @Override
            public short readShort() throws IOException {
                return FilerIO.readShort(_filer, null);
            }

            @Override
            public int readUnsignedShort() throws IOException {
                return FilerIO.readUnsignedShort(_filer, null);
            }

            @Override
            public char readChar() throws IOException {
                return FilerIO.readChar(_filer, null);
            }

            @Override
            public int readInt() throws IOException {
                return FilerIO.readInt(_filer, null);
            }

            @Override
            public long readLong() throws IOException {
                return FilerIO.readLong(_filer, null);
            }

            @Override
            public float readFloat() throws IOException {
                return FilerIO.readFloat(_filer, null);
            }

            @Override
            public double readDouble() throws IOException {
                return FilerIO.readDouble(_filer, null);
            }

            @Override
            public String readLine() throws IOException {
                return FilerIO.readLine(_filer, null);
            }

            @Override
            public String readUTF() throws IOException {
                int length = readUnsignedShort();
                byte[] bytes = new byte[length];
                read(_filer, bytes);
                return new String(bytes, StandardCharsets.UTF_8);
            }
        };
    }

    /**
     * Returns a {@link DataOutput} which delegates to the given {@link Filer}.
     *
     * @param _filer the delegated filer
     * @return a delegating output
     */
    public static DataOutput asDataOutput(final Filer _filer) {
        return new DataOutput() {
            @Override
            public void write(int b) throws IOException {
                _filer.write(b);
            }

            @Override
            public void write(byte[] b) throws IOException {
                _filer.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                _filer.write(b, off, len);
            }

            @Override
            public void writeBoolean(boolean v) throws IOException {
                FilerIO.writeBoolean(_filer, v, null);
            }

            @Override
            public void writeByte(int v) throws IOException {
                FilerIO.writeByte(_filer, v, null);
            }

            @Override
            public void writeShort(int v) throws IOException {
                FilerIO.writeShort(_filer, v, null);
            }

            @Override
            public void writeChar(int v) throws IOException {
                FilerIO.writeChar(_filer, v, null);
            }

            @Override
            public void writeInt(int v) throws IOException {
                FilerIO.writeInt(_filer, v, null);
            }

            @Override
            public void writeLong(long v) throws IOException {
                FilerIO.writeLong(_filer, v, null);
            }

            @Override
            public void writeFloat(float v) throws IOException {
                FilerIO.writeFloat(_filer, v, null);
            }

            @Override
            public void writeDouble(double v) throws IOException {
                FilerIO.writeDouble(_filer, v, null);
            }

            @Override
            public void writeBytes(String s) throws IOException {
                FilerIO.write(_filer, s.getBytes(), null);
            }

            @Override
            public void writeChars(String s) throws IOException {
                FilerIO.writeCharArray(_filer, s.toCharArray(), null);
            }

            @Override
            public void writeUTF(String s) throws IOException {
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                FilerIO.writeShort(_filer, bytes.length, null);
                FilerIO.write(_filer, bytes, null);
            }
        };
    }

}
