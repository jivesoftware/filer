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
 * Used to short circuit GuaranteedDeliveryService
 */
public class NoOpGuaranteedDeliveryService implements GuaranteedDeliveryService {

    public NoOpGuaranteedDeliveryService() {
    }

    @Override
    public void add(List<byte[]> add) throws DeliveryServiceException {
    }

    @Override
    public GuaranteedDeliveryServiceStatus getStatus() {
        return new GuaranteedDeliveryServiceStatus() {
            @Override
            public long undelivered() {
                return 0;
            }

            @Override
            public long delivered() {
                return 0;
            }
        };
    }

    @Override
    public void close() {
    }
}
