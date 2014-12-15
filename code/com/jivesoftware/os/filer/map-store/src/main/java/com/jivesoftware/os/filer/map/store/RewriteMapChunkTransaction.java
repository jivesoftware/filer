package com.jivesoftware.os.filer.map.store;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.RewriteMonkeyFilerTransaction;

/**
 *
 */
public interface RewriteMapChunkTransaction<F extends Filer, R> extends RewriteMonkeyFilerTransaction<MapContext, F, R> {
}
