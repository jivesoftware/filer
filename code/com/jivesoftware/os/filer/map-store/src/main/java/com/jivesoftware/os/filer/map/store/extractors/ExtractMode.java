package com.jivesoftware.os.filer.map.store.extractors;

import com.jivesoftware.os.filer.map.store.MapChunk;

/**
 *
 * @author jonathan.colt
 */
public class ExtractMode implements Extractor<Byte> {

    public static final ExtractMode SINGLETON = new ExtractMode();

    private ExtractMode() {
    }

    @Override
    public Byte extract(int i, long _startIndex, int _keySize, int _payloadSize, MapChunk page) {
        return page.read((int) _startIndex);
    }

    @Override
    public Byte ifNull() {
        return null;
    }

}
