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
public class DeliveryServiceException extends Exception {

    private List<byte[]> failed;

    public DeliveryServiceException(List<byte[]> failed, String message, Throwable cause) {
        super(message, cause);
        this.failed = failed;
    }

    public List<byte[]> getFailed() {
        return failed;
    }
}
