/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c) 2008-2010 Mark Logic Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
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

    /**
     * This version writes multiple documents by calling the write()
     * method for single documents in a loop.  This should be good
     * enough for subclasses that don't have a concept of a txn.
     *
     * @param _outputUri[]
     * @param _contentBytes[][]
     * @param _metadata[]
     * @return
     * 
     * returns the number of Bytes written
     * @throws SyncException 
     */
    public int write(String[] _outputUri, byte[][] _contentBytes,
		     XQSyncDocumentMetadata[] _metadata) throws SyncException
    {
        int bytes = 0; 
        if (null != _outputUri) {
            for (int i = 0; i < _outputUri.length; i++)
                bytes += write(_outputUri[i], _contentBytes[i], _metadata[i]);
        }
        return bytes;
    }
}
