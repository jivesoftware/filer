package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.io.CreateFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.OpenFiler;

/**
 *
 */
public class PartitionDefinition<H, M, F extends Filer> {

    public final ChunkStore chunkStore;
    public final CreateFiler<H, M, F> mapCreator;
    public final OpenFiler<M, F> mapOpener;
    public final GrowFiler<H, M, F> mapGrower;

    public PartitionDefinition(ChunkStore chunkStore,
        CreateFiler<H, M, F> mapCreator,
        OpenFiler<M, F> mapOpener,
        GrowFiler<H, M, F> mapGrower) {
        this.chunkStore = chunkStore;
        this.mapCreator = mapCreator;
        this.mapOpener = mapOpener;
        this.mapGrower = mapGrower;
    }
}
