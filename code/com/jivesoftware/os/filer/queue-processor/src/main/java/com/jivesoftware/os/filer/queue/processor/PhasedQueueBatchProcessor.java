/*
 * $Revision: 142270 $
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.processor;

import com.jivesoftware.os.filer.queue.store.PhasedQueueEntry;
import java.util.List;
/**
 *
 * @author jonathan
 */
public interface PhasedQueueBatchProcessor {

    /**
     * Returns an empty list if all items were processed. Otherwise return the items that weren't processed!
     *
     * @param process
     * @return
     */
    List<PhasedQueueEntry> process(List<PhasedQueueEntry> process);
}
