package com.jivesoftware.os.filer.io.api;

import com.jivesoftware.os.filer.io.IBA;

/**
 *
 * @author jonathan.colt
 */
public class StackBuffer {

    public final byte[] primitiveBuffer = new byte[8];
    private final IBA accessKey = new IBA(primitiveBuffer);

    public IBA accessKey(byte[] key) {
        accessKey.violateImmutability(key);
        return accessKey;
    }
}
