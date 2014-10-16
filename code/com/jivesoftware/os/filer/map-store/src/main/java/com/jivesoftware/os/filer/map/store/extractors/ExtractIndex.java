package com.jivesoftware.os.filer.map.store.extractors;

import com.jivesoftware.os.filer.map.store.MapChunk;

/**
 *
 * @author jonathan.colt
 */
public class ExtractIndex implements Extractor<Integer> {
    
    public static final ExtractIndex SINGLETON = new ExtractIndex();

    private ExtractIndex() {
    }

    @Override
    public Integer extract(int i, long _startIndex, int _keySize, int _payloadSize, MapChunk page) {
        return i;
    }

    @Override
    public Integer ifNull() {
        return -1;
    }

}
