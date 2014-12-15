package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.MonkeyFilerTransaction;

/**
 *
 */
public interface MapTransaction<F extends Filer, R> extends MonkeyFilerTransaction<MapContext, F, R> {
}
