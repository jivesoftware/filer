package com.jivesoftware.os.filer.io.api;

import com.jivesoftware.os.filer.io.api.ChunkTransaction;

/**
 *
 */
public class HintAndTransaction<H, M, R> {
    public final H hint;
    public final ChunkTransaction<M, R> filerTransaction;

    public HintAndTransaction(H hint, ChunkTransaction<M, R> filerTransaction) {
        this.hint = hint;
        this.filerTransaction = filerTransaction;
    }
}
