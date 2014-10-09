package com.jivesoftware.os.filer.map.store.extractors;

/**
 *
 * @author jonathan.colt
 */
public class KeyAndPayload {

    private final byte[] key;
    private final byte[] payload;

    public KeyAndPayload(byte[] key, byte[] payload) {
        this.key = key;
        this.payload = payload;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getPayload() {
        return payload;
    }

}
