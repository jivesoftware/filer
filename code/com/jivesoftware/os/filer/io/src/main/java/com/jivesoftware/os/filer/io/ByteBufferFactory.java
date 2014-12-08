package com.jivesoftware.os.filer.io;

import java.nio.ByteBuffer;

/**
 * @author jonathan
 */
public interface ByteBufferFactory {

    /**
     * @param key
     * @param size
     * @return
     */
    ByteBuffer allocate(byte[] key, long size);

    /**
     * @param key
     * @param oldBuffer can be null and if it is you could have just called allocate which is what implementation should do.
     * @param newSize
     * @return
     */
    ByteBuffer reallocate(byte[] key, ByteBuffer oldBuffer, long newSize);

}
