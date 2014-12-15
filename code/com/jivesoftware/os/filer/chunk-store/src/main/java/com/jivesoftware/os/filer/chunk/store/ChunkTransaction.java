package com.jivesoftware.os.filer.chunk.store;

import java.io.IOException;

/**
 *
 */
public interface ChunkTransaction<M, R> {
    R commit(M monkey, ChunkFiler filer) throws IOException;
}
