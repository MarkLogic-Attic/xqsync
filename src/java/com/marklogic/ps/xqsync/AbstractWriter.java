/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

import java.util.Collection;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.xcc.ContentPermission;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public abstract class AbstractWriter implements WriterInterface {

    protected SimpleLogger logger;

    protected Configuration configuration;

    protected String[] placeKeys;

    protected boolean skipExisting;

    protected boolean repairInputXml;

    protected Collection<ContentPermission> readRoles;

    protected boolean copyProperties;

    protected String[] outputFormatFilters;

    /**
     * @param _configuration
     * @throws SyncException
     */
    public AbstractWriter(Configuration _configuration)
            throws SyncException {
        configuration = _configuration;
        logger = configuration.getLogger();

        copyProperties = configuration.isCopyProperties();
        outputFormatFilters = configuration.getOutputFormatFilters();
        placeKeys = configuration.getPlaceKeys();
        readRoles = configuration.getReadRoles();
        repairInputXml = configuration.isRepairInputXml();
        skipExisting = configuration.isSkipExisting();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.WriterInterface#write(java.lang.String,
     * byte[], com.marklogic.ps.xqsync.XQSyncDocumentMetadata)
     */
    public abstract int write(String uri, byte[] bytes,
            XQSyncDocumentMetadata _metadata) throws SyncException;

}
