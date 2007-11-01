/**
 * Copyright (c) 2004-2007 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

import java.io.File;
import java.util.concurrent.Callable;

import com.marklogic.ps.timing.TimedEvent;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 * This is a lightweight wrapper for CallableSync, to reduce queue size
 */
public class CallableWrapper implements Callable<TimedEvent> {

    private static TaskFactory factory;
    private String uri;
    private File file;
    private InputPackage inputPackage;
    
    /**
     * @param uri
     */
    public CallableWrapper(String uri) {
        this.uri = uri;
    }

    /**
     * @param file
     */
    public CallableWrapper(File file) {
        this.file = file;
    }

    /**
     * @param inputPackage
     * @param path
     */
    public CallableWrapper(InputPackage inputPackage, String path) {
        this.inputPackage = inputPackage;
        this.uri = path;
    }

    /**
     * @param _factory
     */
    public static void setFactory(TaskFactory _factory) {
        factory = _factory;
    }

    /* (non-Javadoc)
     * @see java.util.concurrent.Callable#call()
     */
    public TimedEvent call() throws Exception {
        // basically we do everything at call() time,
        // to reduce memory utilization for the queue
        if (null != inputPackage) {
            return factory.newCallableSync(inputPackage, uri).call();
        }

        if (null != uri) {
            return factory.newCallableSync(uri).call();
        }
        
        return factory.newCallableSync(file).call();        
    }

}
