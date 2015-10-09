package com.jivesoftware.os.filer.io.api;

import java.io.IOException;

/**
 *
 */
public interface IndexAlignedKeyValueTransaction<V> {
    void commit(KeyValueContext<V> keyValueContext, int index) throws IOException;
}
