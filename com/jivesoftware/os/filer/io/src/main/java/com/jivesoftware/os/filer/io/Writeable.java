
package com.jivesoftware.os.filer.io;


import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author Administrator
 */
public interface Writeable extends Closeable {

    /**
     *
     * @param b
     * @throws IOException
     */
    public void write(int b) throws IOException;

    /**
     *
     * @param b
     * @throws IOException
     */
    public void write(byte b[]) throws IOException;

    /**
     *
     * @param b
     * @param _offset
     * @param _len
     * @throws IOException
     */
    public void write(byte b[], int _offset, int _len) throws IOException;
}