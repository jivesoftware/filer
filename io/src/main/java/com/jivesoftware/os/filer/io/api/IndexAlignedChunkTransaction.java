package com.jivesoftware.os.filer.io.api;

import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import java.io.IOException;

/**
 *
 * @param <M>
 */
public interface IndexAlignedChunkTransaction<M> {

    void commit(M monkey, ChunkFiler filer, int index) throws IOException;
}
