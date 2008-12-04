/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
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

    /**
     * @param _configuration
     * @throws SyncException
     */
    public SessionWriter(Configuration _configuration)
            throws SyncException {
        // superclass takes care of some configuration
        super(_configuration);

        session = configuration.newOutputSession();
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

        logger.finest("placeKeys = " + Utilities.join(placeKeys, ","));

        int retries = 3;
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
                options.setPlaceKeys(session.forestNamesToIds(placeKeys));

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
                Thread.yield();
            }
        }
        return _contentBytes.length;
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
