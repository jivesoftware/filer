/*
 * $Revision: 142270 $
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.guaranteed.delivery;

/**
 *
 */
public interface DeliveryCallback {

    /**
     *
     * @param deliver
     * @return
     */
    boolean deliver(Iterable<byte[]> deliver);
}
