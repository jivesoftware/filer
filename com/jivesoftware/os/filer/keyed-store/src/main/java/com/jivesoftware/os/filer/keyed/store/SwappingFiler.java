package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.io.Filer;

/**
 *
 */
public interface SwappingFiler extends Filer {

    void commit() throws Exception;
}
