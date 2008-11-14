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
     * @param _index
     * @param _bytes
     * @throws SyncException
     */
    public void setContent(int _index, byte[] _bytes)
            throws SyncException;

    /**
     * @param _index
     * @param _metadata
     * @throws SyncException
     */
    public void setMetadata(int _index, Reader _metadata)
            throws SyncException;

    /**
     * @throws SyncException
     * 
     */
    public int sync() throws SyncException;

    /**
     * @param _index
     * @param _metadata
     */
    public void setMetadata(int _index, MetadataInterface _metadata);

    /**
     * @return
     */
    public MetadataInterface newMetadata();

    /**
     * @param _index
     * @param _is
     * @throws SyncException
     */
    public void setContent(int _index, InputStream _is)
            throws SyncException;

    /**
     * @param _index
     * @param _reader
     * @throws SyncException
     */
    public void setContent(int _index, Reader _reader)
            throws SyncException;

    /**
     * @param _index
     * @return
     */
    public String getOutputUri(int _index);

    /**
     * @param _index
     */
    public void clearPermissions(int _index);

    /**
     * @param _index
     */
    public void clearProperties(int _index);

}
