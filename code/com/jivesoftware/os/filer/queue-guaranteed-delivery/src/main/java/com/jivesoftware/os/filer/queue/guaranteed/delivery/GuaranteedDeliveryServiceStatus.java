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
 * @author jonathan
 */
public interface GuaranteedDeliveryServiceStatus {

    /**
     * Number of items enqueued for delivery.
     *
     * @return
     */
    long undelivered();

    /**
     * Number of items delivered
     */
    long delivered();
}
