package com.jivesoftware.os.filer.io.api;

import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import java.io.IOException;

/**
 *
 */
public interface IndexAlignedHintAndTransactionSupplier<H, M, R> {

    HintAndTransaction<H, M, R> supply(M monkey,
        ChunkFiler filer,
        StackBuffer stackBuffer,
        Object lock,
        int index) throws IOException, InterruptedException;
}
