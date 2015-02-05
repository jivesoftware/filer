package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.FilerIO;

/**
 *
 */
public class TxPowerConstants {

    private static final ByteArrayStripingLocksProvider SKY_HOOK_POWER_LOCKS = new ByteArrayStripingLocksProvider(256);
    private static final ByteArrayStripingSemaphore SKY_HOOK_POWER_SEMAPHORES = new ByteArrayStripingSemaphore(256, 64);

    public static final MapBackedKeyedFPIndexOpener SKY_HOOK_POWER_OPENER = new MapBackedKeyedFPIndexOpener(SKY_HOOK_POWER_LOCKS, SKY_HOOK_POWER_SEMAPHORES);
    public static final MapBackedKeyedFPIndexGrower SKY_HOOK_POWER_GROWER = new MapBackedKeyedFPIndexGrower(1);
    public static final MapBackedKeyedFPIndexCreator[] SKY_HOOK_POWER_CREATORS = new MapBackedKeyedFPIndexCreator[16];

    static {
        for (int i = 0; i < SKY_HOOK_POWER_CREATORS.length; i++) {
            SKY_HOOK_POWER_CREATORS[i] = new MapBackedKeyedFPIndexCreator(2, (int) FilerIO.chunkLength(i), true, 8, false,
                SKY_HOOK_POWER_OPENER, SKY_HOOK_POWER_LOCKS, SKY_HOOK_POWER_SEMAPHORES);
        }
    }

    private static final ByteArrayStripingLocksProvider POWER_LOCKS = new ByteArrayStripingLocksProvider(256);
    private static final ByteArrayStripingSemaphore POWER_SEMAPHORES = new ByteArrayStripingSemaphore(256, 64);

    public static final MapBackedKeyedFPIndexOpener NAMED_POWER_OPENER = new MapBackedKeyedFPIndexOpener(POWER_LOCKS, POWER_SEMAPHORES);
    public static final MapBackedKeyedFPIndexGrower NAMED_POWER_GROWER = new MapBackedKeyedFPIndexGrower(1);
    public static final MapBackedKeyedFPIndexCreator[] NAMED_POWER_CREATORS = new MapBackedKeyedFPIndexCreator[16];

    static {
        for (int i = 0; i < NAMED_POWER_CREATORS.length; i++) {
            NAMED_POWER_CREATORS[i] = new MapBackedKeyedFPIndexCreator(2, (int) FilerIO.chunkLength(i), true, 8, false,
                NAMED_POWER_OPENER, POWER_LOCKS, POWER_SEMAPHORES);
        }
    }

    public static final SkipListMapBackedKeyedFPIndexOpener SL_NAMED_POWER_OPENER = new SkipListMapBackedKeyedFPIndexOpener(POWER_LOCKS, POWER_SEMAPHORES);
    public static final SkipListMapBackedKeyedFPIndexGrower SL_NAMED_POWER_GROWER = new SkipListMapBackedKeyedFPIndexGrower(1);
    public static final SkipListMapBackedKeyedFPIndexCreator[] SL_NAMED_POWER_CREATORS = new SkipListMapBackedKeyedFPIndexCreator[16];

    static {
        for (int i = 0; i < SL_NAMED_POWER_CREATORS.length; i++) {
            SL_NAMED_POWER_CREATORS[i] = new SkipListMapBackedKeyedFPIndexCreator(2, (int) FilerIO.chunkLength(i), true, 8,
                SL_NAMED_POWER_OPENER, POWER_LOCKS, POWER_SEMAPHORES);
        }
    }

    private TxPowerConstants() {
    }

}
