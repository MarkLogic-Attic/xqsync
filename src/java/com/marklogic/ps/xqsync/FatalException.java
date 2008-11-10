/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class FatalException extends RuntimeException {

    /**
     * @param message
     */
    public FatalException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public FatalException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public FatalException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

}
