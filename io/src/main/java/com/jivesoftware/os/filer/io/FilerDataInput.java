package com.jivesoftware.os.filer.io;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public class FilerDataInput implements DataInput {

    private final Filer filer;
    private final StackBuffer stackBuffer;

    public FilerDataInput(Filer filer, StackBuffer stackBuffer) {
        this.filer = filer;
        this.stackBuffer = stackBuffer;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        int bytesRead = filer.read(b);
        if (bytesRead < b.length) {
            throw new EOFException();
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int bytesRead = filer.read(b, off, len);
        if (bytesRead < len) {
            throw new EOFException();
        }
    }

    @Override
    public int skipBytes(int n) throws IOException {
        long fp = filer.getFilePointer();
        return (int) (filer.skip(n) - fp);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return FilerIO.readBoolean(filer, null);
    }

    @Override
    public byte readByte() throws IOException {
        return FilerIO.readByte(filer, null);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return FilerIO.readUnsignedByte(filer, null);
    }

    @Override
    public short readShort() throws IOException {
        return filer.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return FilerIO.readUnsignedShort(filer, null, stackBuffer);
    }

    @Override
    public char readChar() throws IOException {
        return FilerIO.readChar(filer, null, stackBuffer);
    }

    @Override
    public int readInt() throws IOException {
        return filer.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return filer.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return FilerIO.readFloat(filer, null, stackBuffer);
    }

    @Override
    public double readDouble() throws IOException {
        return FilerIO.readDouble(filer, null, stackBuffer);
    }

    @Override
    public String readLine() throws IOException {
        return FilerIO.readLine(filer, null);
    }

    @Override
    public String readUTF() throws IOException {
        int length = readUnsignedShort();
        byte[] bytes = new byte[length];
        FilerIO.read(filer, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
