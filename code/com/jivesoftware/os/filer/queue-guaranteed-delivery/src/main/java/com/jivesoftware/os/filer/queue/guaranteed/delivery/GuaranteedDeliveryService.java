/*
 * $Revision: 142270 $
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.filer.queue.guaranteed.delivery;

import java.util.List;

/**
 *
 * @author jonathan
 */
public interface GuaranteedDeliveryService {

    /**
     * Adds one batch to be processed by the service. Throws DeliveryServiceException if the batch cannot be added to the underlying storage. This is not the
     * same as failing to be processed. Rather it is failure to be accepted as a candidate for eventual guaranteed delivery.
     * @param add
     */
    void add(List<byte[]> add) throws DeliveryServiceException;

    /**
     * Returns a status object reporting both delivered and un-delivered items.
     * @return
     */
    GuaranteedDeliveryServiceStatus getStatus();

    /**
     * Closes this instance of the service and releases its resources.
     */
    void close();
}
