package com.jivesoftware.os.filer.io.api;

import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import java.io.IOException;

/**
 *
 * @param <M>
 */
public interface IndexAlignedChunkTransaction<M, R> {

    R commit(M monkey, ChunkFiler filer, StackBuffer stackBuffer, Object lock, int index) throws IOException;
}
