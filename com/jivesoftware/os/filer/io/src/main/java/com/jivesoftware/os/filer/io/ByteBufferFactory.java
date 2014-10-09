package com.jivesoftware.os.filer.io;

import java.nio.ByteBuffer;

/**
 *
 * @author jonathan
 */
public interface ByteBufferFactory {

    /**
     *
     * @param _size
     * @return
     */
    ByteBuffer allocate(long _size);

}
