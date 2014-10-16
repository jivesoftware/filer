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
    
    /**
     * 
     * @param oldBuffer can be null and if it is you could have just called allocate which is what implementation should do.
     * @param newSize
     * @return 
     */
    ByteBuffer reallocate(ByteBuffer oldBuffer, long newSize);

}
