package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.FilerIO;

/**
 *
 */
public class TxPowerConstants {

    private static final ByteArrayStripingLocksProvider SKY_HOOK_POWER_LOCKS = new ByteArrayStripingLocksProvider(256);
    public static final MapBackedKeyedFPIndexOpener SKY_HOOK_POWER_OPENER = new MapBackedKeyedFPIndexOpener(SKY_HOOK_POWER_LOCKS);
    public static final MapBackedKeyedFPIndexCreator[] SKY_HOOK_POWER_CREATORS = new MapBackedKeyedFPIndexCreator[16];

    static {
        for (int i = 0; i < SKY_HOOK_POWER_CREATORS.length; i++) {
            SKY_HOOK_POWER_CREATORS[i] = new MapBackedKeyedFPIndexCreator(2, (int) FilerIO.chunkLength(i), true, 8, false,
                SKY_HOOK_POWER_OPENER, SKY_HOOK_POWER_LOCKS);
        }
    }

    private static final ByteArrayStripingLocksProvider NAMED_POWER_LOCKS = new ByteArrayStripingLocksProvider(256);
    public static final MapBackedKeyedFPIndexOpener NAMED_POWER_OPENER = new MapBackedKeyedFPIndexOpener(NAMED_POWER_LOCKS);
    public static final MapBackedKeyedFPIndexCreator[] NAMED_POWER_CREATORS = new MapBackedKeyedFPIndexCreator[16];

    static {
        for (int i = 0; i < NAMED_POWER_CREATORS.length; i++) {
            NAMED_POWER_CREATORS[i] = new MapBackedKeyedFPIndexCreator(2, (int) FilerIO.chunkLength(i), true, 8, false,
                NAMED_POWER_OPENER, NAMED_POWER_LOCKS);
        }
    }

}
