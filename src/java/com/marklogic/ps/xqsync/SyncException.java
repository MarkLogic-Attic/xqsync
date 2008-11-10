/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 */
public class SyncException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param cause
     */
    public SyncException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public SyncException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public SyncException(String message) {
        super(message);
    }

}
