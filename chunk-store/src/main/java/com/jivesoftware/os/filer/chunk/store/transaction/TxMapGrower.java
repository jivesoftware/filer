package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.api.ChunkTransaction;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.map.MapContext;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class TxMapGrower {

    public static final TxNamedMapOfFilerOverwriteGrowerProvider<Integer, MapContext> MAP_OVERWRITE_GROWER = hint -> new MapGrower<>();

    public static final TxNamedMapOfFilerRewriteGrowerProvider<Integer, MapContext> MAP_REWRITE_GROWER =
        new TxNamedMapOfFilerRewriteGrowerProvider<Integer, MapContext>() {

        @Override
        public <R> GrowFiler<Integer, MapContext, ChunkFiler> create(final Integer hint,
            final ChunkTransaction<MapContext, R> chunkTransaction,
            final AtomicReference<R> result) {

            final MapGrower<MapContext> mapGrower = new MapGrower<>();
            return new GrowFiler<Integer, MapContext, ChunkFiler>() {

                @Override
                public Integer acquire(Integer hint, MapContext monkey, ChunkFiler filer, Object lock) throws IOException {
                    return mapGrower.acquire(hint, monkey, filer, lock);
                }

                @Override
                public void growAndAcquire(Integer hint,
                    MapContext currentMonkey,
                    ChunkFiler currentFiler,
                    MapContext newMonkey,
                    ChunkFiler newFiler,
                    Object currentLock,
                    Object newLock,
                    byte[] primitiveBuffer) throws IOException {

                    mapGrower.growAndAcquire(hint, currentMonkey, currentFiler, newMonkey, newFiler, currentLock, newLock, primitiveBuffer);

                    result.set(chunkTransaction.commit(newMonkey, newFiler, primitiveBuffer, newLock));
                }

                @Override
                public void release(Integer hint, MapContext monkey, Object lock) {
                    mapGrower.release(hint, monkey, lock);
                }
            };
        }
    };

}
