package com.jivesoftware.os.filer.io.api;

import java.io.IOException;

/**
 *
 */
public class CorruptionException extends IOException {
    public CorruptionException(String message) {
        super(message);
    }
}
