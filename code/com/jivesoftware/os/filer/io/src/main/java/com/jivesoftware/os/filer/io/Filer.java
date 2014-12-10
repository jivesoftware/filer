package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 * @author Administrator
 */
public interface Filer extends Readable, Writeable {

    /**
     *
     */
    final public static String cRead = "r";
    /**
     *
     */
    final public static String cWrite = "rw";
    /**
     *
     */
    final public static String cReadWrite = "rw";

    /**
     *
     * @return
     * @throws IOException
     */
    public Object lock() throws IOException;

    /**
     *
     * @param position
     * @throws IOException
     */
    void seek(long position) throws IOException;

    /**
     *
     * @param position
     * @return
     * @throws IOException
     */
    long skip(long position) throws IOException;

    /**
     *
     * @return @throws IOException
     */
    long length() throws IOException;

    /**
     *
     * @param len
     * @throws IOException
     */
    void setLength(long len) throws IOException;

    /**
     *
     * @return @throws IOException
     */
    long getFilePointer() throws IOException;

    /**
     *
     * @throws IOException
     */
    void eof() throws IOException;

    /**
     *
     * @throws IOException
     */
    void flush() throws IOException;

}
