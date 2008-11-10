/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

import java.io.InputStream;
import java.io.Reader;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public interface DocumentInterface {

    /**
     * @throws SyncException
     * 
     */
    public void setContent(byte[] _bytes) throws SyncException;

    /**
     * @throws SyncException
     * 
     */
    public void setMetadata(Reader _metadata) throws SyncException;

    /**
     * @throws SyncException
     * 
     */
    public int sync() throws SyncException;

    /**
     * @param _metadata
     */
    public void setMetadata(MetadataInterface _metadata);

    /**
     * @return
     */
    public MetadataInterface newMetadata();

    /**
     * @param _is
     * @throws SyncException 
     */
    public void setContent(InputStream _is) throws SyncException;

    /**
     * @param _reader
     * @throws SyncException 
     */
    public void setContent(Reader _reader) throws SyncException;

    /**
     * @return
     */
    public String getOutputUri();

    /**
     * 
     */
    public void clearPermissions();

    /**
     * 
     */
    public void clearProperties();

}
