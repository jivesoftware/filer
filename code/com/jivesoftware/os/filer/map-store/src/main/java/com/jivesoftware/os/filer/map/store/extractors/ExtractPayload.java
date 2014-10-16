package com.jivesoftware.os.filer.map.store.extractors;

import com.jivesoftware.os.filer.map.store.MapChunk;

/**
 *
 * @author jonathan.colt
 */
public class ExtractPayload implements Extractor<byte[]> {

    public static final ExtractPayload SINGLETON = new ExtractPayload();

    private ExtractPayload() {
    }

    @Override
    public byte[] extract(int i, long _startIndex, int _keySize, int _payloadSize, MapChunk page) {
        byte[] p = new byte[_payloadSize];
        page.read((int) _startIndex + 1 + _keySize, p, 0, _payloadSize);
        return p;
    }

    @Override
    public byte[] ifNull() {
        return null;
    }

}
