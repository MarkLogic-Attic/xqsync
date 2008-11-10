/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public interface WriterInterface {

    /**
     * @param _outputUri
     * @param _contentBytes
     * @param _metadata
     * @return
     * 
     * returns the number of Bytes written
     * @throws SyncException 
     */
    int write(String _outputUri, byte[] _contentBytes,
            XQSyncDocumentMetadata _metadata) throws SyncException;

}
