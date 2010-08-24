/**
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

import java.util.Arrays;

import com.marklogic.ps.Session;
import com.marklogic.ps.Utilities;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.DocumentRepairLevel;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 */
/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class SessionWriter extends AbstractWriter {

    protected Session session;

    protected static Object firstMaxTasksMutex = new Object();

    protected static boolean firstMaxTasks = false;

    /**
     * @param _configuration
     * @throws SyncException
     */
    public SessionWriter(Configuration _configuration)
            throws SyncException {
        // superclass takes care of some configuration
        super(_configuration);

        session = configuration.newOutputSession();
        if (null == session) {
            throw new FatalException("null output session");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.WriterInterface#write(java.lang.String,
     * byte[], com.marklogic.ps.xqsync.XQSyncDocumentMetadata)
     */
    public int write(String _outputUri, byte[] _contentBytes,
            XQSyncDocumentMetadata _metadata) throws SyncException {

        if (matchesFilters(_outputUri, _metadata)) {
            return 0;
        }

        if (null == session) {
            throw new FatalException("null session");
        }

        logger.finest("placeKeys = " + Utilities.join(placeKeys, ","));

        int retries = 3;
        long sleepMillis = 125;
        // in case the server is unreliable, we try three times
        while (retries > 0) {
            try {
                // handle deletes
                if (null == _contentBytes || _contentBytes.length < 1) {
                    // this document has been deleted
                    session.deleteDocument(_outputUri);
                    return 0;
                }

                // optionally, check to see if document is already up-to-date
                if (skipExisting && session.existsDocument(_outputUri)) {
                    logger.fine("skipping existing document: "
                            + _outputUri);
                    return 0;
                }

                // constants
                boolean resolveEntities = false;
                String namespace = null;

                DocumentRepairLevel repair = (!repairInputXml) ? DocumentRepairLevel.NONE
                        : DocumentRepairLevel.FULL;
                logger.fine("repair = " + repairInputXml + ", " + repair);

                // marshal the permissions as an array
                // don't check copyProperties here:
                // if false, the constructor shouldn't have read any
                // and anyway we still want to handle any _readRoles
                _metadata.addPermissions(readRoles);
                ContentPermission[] permissions = _metadata
                        .getPermissions();
                String[] collections = _metadata.getCollections();
                logger.fine("collections = "
                        + Utilities.join(collections, " "));

                ContentCreateOptions options = null;
                if (_metadata.isBinary()) {
                    logger.fine(_outputUri + " is binary");
                    options = ContentCreateOptions.newBinaryInstance();
                } else if (_metadata.isText()) {
                    logger.fine(_outputUri + " is text");
                    options = ContentCreateOptions.newTextInstance();
                } else {
                    logger.fine(_outputUri + " is xml");
                    options = ContentCreateOptions.newXmlInstance();
                }

                options.setResolveEntities(resolveEntities);
                if (null != permissions) {
                    options.setPermissions(permissions);
                }
                options.setCollections(collections);
                options.setQuality(_metadata.getQuality());
                options.setNamespace(namespace);
                options.setRepairLevel(repair);
                if (null != placeKeys) {
                    options.setPlaceKeys(session
                            .forestNamesToIds(placeKeys));
                }

                Content content = ContentFactory.newContent(_outputUri,
                        _contentBytes, options);

                session.insertContent(content);
                session.commit();

                // handle prop:properties node, optional
                // TODO do this in the same transaction
                if (copyProperties) {
                    // logger.info("copying properties for " + _outputUri);
                    String properties = _metadata.getProperties();
                    if (null != properties) {
                        session.setDocumentProperties(_outputUri,
                                properties);
                    }
                }
                // success - will not loop again
                break;
            } catch (XQueryException e) {
                if ("XDMP-MAXTASKS".equals(e.getCode())) {
                    // retry, without limit
                    if (!firstMaxTasks) {
                        synchronized (firstMaxTasksMutex) {
                            if (!firstMaxTasks) {
                                firstMaxTasks = true;
                                logger
                                        .warning("XDMP-MAXTASKS seen - will retry"
                                                + " (appears only once per run)");
                            }
                        }
                    }
                    sleepMillis = sleepForRetry(sleepMillis);
                    continue;
                }
                throw new SyncException(e);
            } catch (XccException e) {
                retries--;
                // we want to know which document it was
                if (retries < 1) {
                    throw new SyncException("retries exhausted for "
                            + _outputUri, e);
                }
                logger.logException(
                        "error writing document: will retry (" + retries
                                + "): " + _outputUri, e);
                sleepMillis = sleepForRetry(sleepMillis);
            }
        }
        return _contentBytes.length;
    }

    /**
     * @param sleepMillis
     * @return
     */
    private long sleepForRetry(long sleepMillis) {
        logger.fine("sleepMillis = " + sleepMillis);
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            // reset interrupt status and continue
            Thread.interrupted();
            logger.logException(
                    "interrupted during sleep " + sleepMillis, e);
        }
        return 2 * sleepMillis;
    }

    /**
     * @param _outputUri
     * @param _metadata
     * @return
     */
    private boolean matchesFilters(String _outputUri,
            MetadataInterface _metadata) {
        // check format - return true if any filter matches
        if (null != outputFormatFilters
                && Arrays.binarySearch(outputFormatFilters, _metadata
                        .getFormatName()) > -1) {
            logger.finer(Configuration.OUTPUT_FILTER_FORMATS_KEY
                    + " matched " + _outputUri);
            return true;
        }

        return false;
    }

}
