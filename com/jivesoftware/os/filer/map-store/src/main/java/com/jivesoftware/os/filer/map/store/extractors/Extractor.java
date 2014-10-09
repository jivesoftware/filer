package com.jivesoftware.os.filer.map.store.extractors;

import com.jivesoftware.os.filer.map.store.MapChunk;

/**
 *
 * @param <R>
 */
public interface Extractor<R> {

    R extract(int i, long _startIndex, int _keySize, int _payloadSize, MapChunk page);

    R ifNull();

}
