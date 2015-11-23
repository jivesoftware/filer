package com.jivesoftware.os.filer.io;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public class FilerDataOutput implements DataOutput {

    private final Filer filer;
    private final StackBuffer stackBuffer;

    public FilerDataOutput(Filer filer, StackBuffer stackBuffer) {
        this.filer = filer;
        this.stackBuffer = stackBuffer;
    }

    @Override
    public void write(int b) throws IOException {
        filer.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        filer.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        filer.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        FilerIO.writeBoolean(filer, v, null);
    }

    @Override
    public void writeByte(int v) throws IOException {
        FilerIO.writeByte(filer, v, null);
    }

    @Override
    public void writeShort(int v) throws IOException {
        FilerIO.writeShort(filer, v, null, stackBuffer);
    }

    @Override
    public void writeChar(int v) throws IOException {
        FilerIO.writeChar(filer, v, null, stackBuffer);
    }

    @Override
    public void writeInt(int v) throws IOException {
        FilerIO.writeInt(filer, v, null, stackBuffer);
    }

    @Override
    public void writeLong(long v) throws IOException {
        FilerIO.writeLong(filer, v, null, stackBuffer);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        FilerIO.writeFloat(filer, v, null, stackBuffer);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        FilerIO.writeDouble(filer, v, null, stackBuffer);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        FilerIO.write(filer, s.getBytes(), null);
    }

    @Override
    public void writeChars(String s) throws IOException {
        FilerIO.writeCharArray(filer, s.toCharArray(), null, stackBuffer);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        FilerIO.writeShort(filer, bytes.length, null, stackBuffer);
        FilerIO.write(filer, bytes, null);
    }
}
