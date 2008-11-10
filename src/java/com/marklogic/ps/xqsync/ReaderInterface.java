/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;


/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public interface ReaderInterface {

    /**
     * @param _uri
     * @param _document
     * @throws SyncException
     */
    public void read(String _uri, DocumentInterface _document)
            throws SyncException;
    
    public void close();

}
