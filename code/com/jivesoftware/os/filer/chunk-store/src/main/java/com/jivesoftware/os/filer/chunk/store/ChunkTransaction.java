package com.jivesoftware.os.filer.chunk.store;

import java.io.IOException;

/**
 *
 * @param <M>
 * @param <R>
 */
public interface ChunkTransaction<M, R> {

    R commit(M monkey, ChunkFiler filer, Object lock) throws IOException;
}
