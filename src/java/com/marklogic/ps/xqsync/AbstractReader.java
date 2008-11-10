/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

import com.marklogic.ps.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public abstract class AbstractReader implements ReaderInterface {

    protected SimpleLogger logger;

    protected Configuration configuration;

    /**
     * @param _configuration
     * @throws SyncException
     */
    public AbstractReader(Configuration _configuration)
            throws SyncException {
        configuration = _configuration;
        logger = configuration.getLogger();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.ReaderInterface#read(java.lang.String,
     * com.marklogic.ps.xqsync.DocumentInterface)
     */
    public abstract void read(String _uri, DocumentInterface _document)
            throws SyncException;

    public void close() {
        // do nothing
    }

}
