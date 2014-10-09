package com.jivesoftware.os.filer.io;

import java.io.Closeable;
import java.io.IOException;

/**

@author jonathan.colt
*/
public interface Readable extends Closeable {

    /**
     *
     * @return
     * @throws IOException
     */
    public int read() throws IOException;

    /**
     *
     * @param b
     * @return
     * @throws IOException
     */
    public int read(byte b[]) throws IOException;

    /**
     *
     * @param b
     * @param _offset
     * @param _len
     * @return
     * @throws IOException
     */
    public int read(byte b[], int _offset, int _len) throws IOException;
}