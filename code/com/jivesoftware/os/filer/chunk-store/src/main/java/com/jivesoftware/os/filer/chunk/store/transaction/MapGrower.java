package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.map.store.MapContext;
import com.jivesoftware.os.filer.map.store.MapStore;
import java.io.IOException;

/**
 * @author jonathan.colt
 * @param <M>
 */
public class MapGrower<M extends MapContext> implements GrowFiler<Integer, M, ChunkFiler> {

    private final int alwaysRoomForNMoreKeys;

    public MapGrower(int alwaysRoomForNMoreKeys) {
        this.alwaysRoomForNMoreKeys = alwaysRoomForNMoreKeys;
    }

    @Override
    public Integer grow(M monkey, ChunkFiler filer) throws IOException {
        synchronized (monkey) {
            if (MapStore.INSTANCE.isFullWithNMore(filer, monkey, alwaysRoomForNMoreKeys)) {
                return MapStore.INSTANCE.nextGrowSize(monkey, alwaysRoomForNMoreKeys);
            }
        }
        return null;
    }

    @Override
    public void grow(M currentMonkey, ChunkFiler currentFiler, M newMonkey, ChunkFiler newFiler) throws IOException {
        synchronized (currentMonkey) {
            synchronized (newMonkey) {
                MapStore.INSTANCE.copyTo(currentFiler, currentMonkey, newFiler, newMonkey, null);
            }
        }
    }
}
