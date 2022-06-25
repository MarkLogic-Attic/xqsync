/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c) 2008-2022 MarkLogic Corporation. All rights reserved.
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
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public abstract class AbstractWriter implements WriterInterface {

    protected final SimpleLogger logger;
    protected final Configuration configuration;
    protected final String[] placeKeys;
    protected final boolean skipExisting;
    protected final boolean repairInputXml;
    protected final Collection<ContentPermission> permissionRoles;
    protected final boolean copyProperties;
    protected final String[] outputFormatFilters;

    /**
     * @param configuration
     */
    protected AbstractWriter(Configuration configuration) {
        this.configuration = configuration;
        logger = configuration.getLogger();
        copyProperties = configuration.isCopyProperties();
        outputFormatFilters = configuration.getOutputFormatFilters();
        placeKeys = configuration.getPlaceKeys();
        permissionRoles = configuration.getPermissionRoles();
        repairInputXml = configuration.isRepairInputXml();
        skipExisting = configuration.isSkipExisting();
    }

    /**
     * This version writes multiple documents by calling the write()
     * method for single documents in a loop.  This should be good
     * enough for subclasses that don't have a concept of a txn.
     *
     * @param outputUri
     * @param contentBytes
     * @param metadata
     * @return
     *
     * returns the number of Bytes written
     * @throws SyncException
     */
    public int write(String[] outputUri, byte[][] contentBytes, XQSyncDocumentMetadata[] metadata) throws SyncException {
        int bytes = 0;
        if (null != outputUri) {
            for (int i = 0; i < outputUri.length; i++) {
                bytes += write(outputUri[i], contentBytes[i], metadata[i]);
            }
        }
        return bytes;
    }
}
