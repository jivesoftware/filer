package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;

/**
 *
 */
public class TxCogs {

    private static final int NUM_POWERS = 16;

    private final ByteArrayStripingLocksProvider skyHookPowerLocks;
    private final ByteArrayStripingSemaphore skyHookPowerSemaphores;

    private final ByteArrayStripingLocksProvider powerLocks;
    private final ByteArrayStripingSemaphore powerSemaphores;

    private final IntIndexSemaphore skyHookKeySemaphores;
    private final IntIndexSemaphore namedKeySemaphores;

    private final KeyToFPCacheFactory skyHookFpCacheFactory;
    private final KeyToFPCacheFactory namedPowerFpCacheFactory;
    private final KeyToFPCacheFactory slNamedPowerCacheFactory;

    public TxCogs(int stripingLevel,
        int numSemaphorePermits,
        KeyToFPCacheFactory skyHookFpCacheFactory,
        KeyToFPCacheFactory namedPowerFpCacheFactory,
        KeyToFPCacheFactory slNamedPowerCacheFactory) {

        this.skyHookFpCacheFactory = skyHookFpCacheFactory;
        this.namedPowerFpCacheFactory = namedPowerFpCacheFactory;
        this.slNamedPowerCacheFactory = slNamedPowerCacheFactory;

        skyHookPowerLocks = new ByteArrayStripingLocksProvider(stripingLevel);
        skyHookPowerSemaphores = new ByteArrayStripingSemaphore(stripingLevel, numSemaphorePermits);

        powerLocks = new ByteArrayStripingLocksProvider(stripingLevel);
        powerSemaphores = new ByteArrayStripingSemaphore(stripingLevel, numSemaphorePermits);

        skyHookKeySemaphores = new IntIndexSemaphore(stripingLevel, numSemaphorePermits);
        namedKeySemaphores = new IntIndexSemaphore(stripingLevel, numSemaphorePermits);
    }

    public TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> getSkyhookCog(int seed) {
        MapBackedKeyedFPIndexCreator[] creators = new MapBackedKeyedFPIndexCreator[NUM_POWERS];
        MapBackedKeyedFPIndexOpener opener = new MapBackedKeyedFPIndexOpener(seed, skyHookPowerLocks, skyHookPowerSemaphores, skyHookFpCacheFactory);
        MapBackedKeyedFPIndexGrower grower = new MapBackedKeyedFPIndexGrower();
        for (int i = 0; i < creators.length; i++) {
            creators[i] = new MapBackedKeyedFPIndexCreator(seed, 2, (int) FilerIO.chunkLength(i), true, 8, false,
                opener, skyHookPowerLocks, skyHookPowerSemaphores, skyHookFpCacheFactory);
        }
        return new TxCog<>(creators, opener, grower);
    }

    public TxCog<Integer, MapBackedKeyedFPIndex, ChunkFiler> getPowerCog(int seed) {
        MapBackedKeyedFPIndexCreator[] creators = new MapBackedKeyedFPIndexCreator[NUM_POWERS];
        MapBackedKeyedFPIndexOpener opener = new MapBackedKeyedFPIndexOpener(seed, powerLocks, powerSemaphores, namedPowerFpCacheFactory);
        MapBackedKeyedFPIndexGrower grower = new MapBackedKeyedFPIndexGrower();
        for (int i = 0; i < creators.length; i++) {
            creators[i] = new MapBackedKeyedFPIndexCreator(seed, 2, (int) FilerIO.chunkLength(i), true, 8, false,
                opener, powerLocks, powerSemaphores, namedPowerFpCacheFactory);
        }
        return new TxCog<>(creators, opener, grower);
    }

    public TxCog<Integer, SkipListMapBackedKeyedFPIndex, ChunkFiler> getSkipListPowerCog(int seed) {
        SkipListMapBackedKeyedFPIndexCreator[] creators = new SkipListMapBackedKeyedFPIndexCreator[NUM_POWERS];
        SkipListMapBackedKeyedFPIndexOpener opener = new SkipListMapBackedKeyedFPIndexOpener(seed, powerLocks, powerSemaphores, slNamedPowerCacheFactory);
        SkipListMapBackedKeyedFPIndexGrower grower = new SkipListMapBackedKeyedFPIndexGrower();
        for (int i = 0; i < creators.length; i++) {
            creators[i] = new SkipListMapBackedKeyedFPIndexCreator(seed, 2, (int) FilerIO.chunkLength(i), true, 8,
                opener, powerLocks, powerSemaphores, slNamedPowerCacheFactory);
        }
        return new TxCog<>(creators, opener, grower);
    }

    public IntIndexSemaphore getSkyHookKeySemaphores() {
        return skyHookKeySemaphores;
    }

    public IntIndexSemaphore getNamedKeySemaphores() {
        return namedKeySemaphores;
    }
}
