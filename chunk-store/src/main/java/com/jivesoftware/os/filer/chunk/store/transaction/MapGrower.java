package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.io.GrowFiler;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import java.io.IOException;

/**
 * @param <M>
 * @author jonathan.colt
 */
public class MapGrower<M extends MapContext> implements GrowFiler<Integer, M, ChunkFiler> {

    @Override
    public Integer acquire(Integer sizeHint, M monkey, ChunkFiler filer, Object lock) throws IOException {
        synchronized (lock) {
            if (MapStore.INSTANCE.acquire(monkey, sizeHint)) {
                return null;
            } else {
                return MapStore.INSTANCE.nextGrowSize(monkey, sizeHint);
            }
        }
    }

    @Override
    public void growAndAcquire(Integer sizeHint,
        M currentMonkey,
        ChunkFiler currentFiler,
        M newMonkey,
        ChunkFiler newFiler,
        Object currentLock,
        Object newLock) throws IOException {

        synchronized (currentLock) {
            synchronized (newLock) {
                if (MapStore.INSTANCE.acquire(newMonkey, sizeHint)) {
                    MapStore.INSTANCE.copyTo(currentFiler, currentMonkey, newFiler, newMonkey, null);
                } else {
                    throw new RuntimeException("Newly allocated MapGrower context does not have necessary capacity!");
                }
            }
        }
    }

    @Override
    public void release(Integer sizeHint, M monkey, Object lock) {
        synchronized (lock) {
            MapStore.INSTANCE.release(monkey, sizeHint);
        }
    }
}
