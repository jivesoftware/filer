package com.jivesoftware.os.filer.keyed.store;

import com.jivesoftware.os.filer.io.Filer;
import java.io.IOException;

/**
 *
 */
public interface SwappableFiler extends Filer {

    /**
     * Must be called after acquiring the {@link #lock()}. Synchronizes this filer in case it has been
     * swapped by another thread.
     */
    void sync() throws IOException;

    /**
     * Start a swapping filer. Finish by calling {@link SwappingFiler#commit()}.
     *
     * @param initialChunkSize the size if a new filer must be created
     * @return the swapping filer
     * @throws Exception
     */
    SwappingFiler swap(long initialChunkSize) throws Exception;
}
