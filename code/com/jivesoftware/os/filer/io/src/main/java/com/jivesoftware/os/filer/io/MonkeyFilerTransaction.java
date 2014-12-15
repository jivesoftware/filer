package com.jivesoftware.os.filer.io;

import java.io.IOException;

/**
 *
 */
public interface MonkeyFilerTransaction<M, F extends Filer, R> {

    R commit(M monkey, F filer) throws IOException;
}
