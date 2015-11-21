package com.jivesoftware.os.filer.io.api;

import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import java.io.IOException;

/**
 *
 * @param <M>
 * @param <R>
 */
public interface ChunkTransaction<M, R> {

    R commit(M monkey, ChunkFiler filer, StackBuffer stackBuffer, Object lock) throws IOException;
}
