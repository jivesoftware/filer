package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.chunk.store.ChunkFiler;
import com.jivesoftware.os.filer.io.GrowFiler;
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
    public Integer acquire(M monkey, ChunkFiler filer) throws IOException {
        synchronized (monkey) {
            if (MapStore.INSTANCE.acquire(monkey, alwaysRoomForNMoreKeys)) {
                return null;
            } else {
                return MapStore.INSTANCE.nextGrowSize(monkey, alwaysRoomForNMoreKeys);
            }
        }
    }

    @Override
    public void growAndAcquire(M currentMonkey, ChunkFiler currentFiler, M newMonkey, ChunkFiler newFiler) throws IOException {
        synchronized (currentMonkey) {
            synchronized (newMonkey) {
                if (MapStore.INSTANCE.acquire(newMonkey, alwaysRoomForNMoreKeys)) {
                    MapStore.INSTANCE.copyTo(currentFiler, currentMonkey, newFiler, newMonkey, null);
                } else {
                    throw new RuntimeException("Newly allocated MapGrower context does not have necessary capacity!");
                }
            }
        }
    }

    @Override
    public void release(M monkey) {
        synchronized (monkey) {
            MapStore.INSTANCE.release(monkey, alwaysRoomForNMoreKeys);
        }
    }
}
