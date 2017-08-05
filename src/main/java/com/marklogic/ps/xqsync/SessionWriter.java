/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c) 2008-2012 MarkLogic Corporation. All rights reserved.
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
import java.util.ArrayList;
import java.util.Random;
import java.util.Map;
import java.math.BigInteger;
import java.security.MessageDigest;

import com.marklogic.ps.Session;
import com.marklogic.ps.Utilities;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.DocumentRepairLevel;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class SessionWriter extends AbstractWriter {

    protected static Object firstMaxTasksMutex = new Object();

    protected static boolean firstMaxTasks = false;

    protected Map<String, BigInteger> forestMap = null;

    protected String forestNameArray[] = null;

    protected int last_batch_size = -1;
    protected String query = null;

    protected int maxRetries = 3;

   /**
     * @param _configuration
     * @throws SyncException
     */
    public SessionWriter(Configuration _configuration)
            throws SyncException {
        // superclass takes care of some configuration
        super(_configuration);

        // prepare for in-forest eval
        if (configuration.useInForestEval()) {
            forestMap = configuration.getOutputForestMap();
            if (forestMap == null)
                throw new SyncException("cannot retrieve forest map");

            forestNameArray = placeKeys;
            if (forestNameArray == null || forestNameArray.length == 0)
                forestNameArray = configuration.getOutputForestNames();
        }

        maxRetries = configuration.getMaxRetries();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.ps.xqsync.WriterInterface#write(java.lang.String,
     * byte[], com.marklogic.ps.xqsync.XQSyncDocumentMetadata)
     */
    public int write(String _outputUri, byte[] _contentBytes,
            XQSyncDocumentMetadata _metadata) throws SyncException {
        String _outputUriArray[] = new String[1];
        byte _contentBytesArray[][] = new byte[1][0];
        XQSyncDocumentMetadata _metadataArray[] = new XQSyncDocumentMetadata[1];

        _outputUriArray[0] = _outputUri;
        _contentBytesArray[0] = _contentBytes;
        _metadataArray[0] = _metadata;
        return write(_outputUriArray, _contentBytesArray, _metadataArray);
    }

    /**
     * This version writes multiple documents in a single txn.
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
        boolean useInForestEval = configuration.useInForestEval();

        // do nothing if there's no input
        if (null == _outputUri || _outputUri.length == 0)
            return bytes;

        // check which ones we really need to process by running
        // everything through filtes
        boolean ignoreList[] = new boolean[_outputUri.length];
        for (int i = 0; i < _outputUri.length; i++)
            ignoreList[i] = matchesFilters(_outputUri[i], _metadata[i]);

        // ignore the ones that have no uri
        for (int i = 0; i < _outputUri.length; i++)
            if (!ignoreList[i] && (null == _outputUri[i] || _outputUri[i].isEmpty()))
                ignoreList[i] = true;

        // create the session to be used
        String forestName = null;
        BigInteger forestIdBigInt = null;
        Session session = null;

        if (!useInForestEval) {
            session = configuration.newOutputSession();
        } else {
            // This provides repeatable placement
            // as long as uris arrive in repeatable batches.
            // TODO how uniform will placement be? use a different hash?
            int evalForestIdx = Math.abs(_outputUri[0].hashCode()
                                         % forestNameArray.length);
            forestName = forestNameArray[evalForestIdx];
            forestIdBigInt = forestMap.get(forestName);
            if (null == forestIdBigInt) {
                throw new FatalException("forest " + forestName + " not found");
            }
            session = configuration.newOutputSession("#"+forestIdBigInt.toString());
        }

        if (null == session) {
            throw new FatalException("null output session");
        }

        // handle delete requests.  These are URIs that has 0
        // contentBytes
        if (!useInForestEval) {
            for (int i = 0; i < _outputUri.length; i++) {
                if (!ignoreList[i] &&
                    (null == _contentBytes[i] || _contentBytes[i].length < 1)) {
                    ignoreList[i] = true;
                    try {
                        session.deleteDocument(_outputUri[i]);
                    } catch (XccException e) {
                        // don't retry delete.  We simply log the failure
                        logger.logException("error deleting document: " +
                                            _outputUri[i], e);
                    }
                }
            }

            // skip existing documents if requested
            if (skipExisting) {
                for (int i = 0; i < _outputUri.length; i++) {
                    if (!ignoreList[i]) {
                        try {
                            if (session.existsDocument(_outputUri[i]))
                                ignoreList[i] = true;
                        } catch (XccException e) {
                            // don't retry this.  We simply log the failure
                            logger.logException("error on check existing document: " +
                                                _outputUri[i], e);
                        }
                    }
                }
            }
        }

        // create the contents to be inserted
        ArrayList<Content> contentList = new ArrayList<Content>(_outputUri.length);
        for (int i = 0; i < _outputUri.length; i++) {
            if (ignoreList[i])
                continue;

            ContentCreateOptions options = null;
            if (_metadata[i].isBinary()) {
                logger.fine(_outputUri[i] + " is binary");
                options = ContentCreateOptions.newBinaryInstance();
            } else if (_metadata[i].isText()) {
                logger.fine(_outputUri[i] + " is text");
                options = ContentCreateOptions.newTextInstance();
            } else {
                logger.fine(_outputUri[i] + " is xml");
                options = ContentCreateOptions.newXmlInstance();
            }

            // resolve entities, this seems to be always set to false
            options.setResolveEntities(false);

            // permissions
            _metadata[i].addPermissions(permissionRoles);
            ContentPermission[] permissions = _metadata[i].getPermissions();
            if (null != permissions)
                options.setPermissions(permissions);

            // collections
            String[] collections = _metadata[i].getCollections();
            logger.fine("collections = "
                        + Utilities.join(collections, " "));
            options.setCollections(collections);

            // quality
            options.setQuality(_metadata[i].getQuality());

            // namespace, seems to be always null
            options.setNamespace(null);

            // repair level
            DocumentRepairLevel repair = (!repairInputXml) ?
                DocumentRepairLevel.NONE : DocumentRepairLevel.FULL;
            logger.fine("repair = " + repairInputXml + ", " + repair);
            options.setRepairLevel(repair);

            // place keys
            if (null != placeKeys) {
                try {
                    if (forestIdBigInt == null) {
                        logger.finest("placeKeys = " +
                                      Utilities.join(placeKeys, ","));
                        options.setPlaceKeys(session.forestNamesToIds(placeKeys));
                    } else {
                        BigInteger forestIds[] = {forestIdBigInt};
                        options.setPlaceKeys(forestIds);
                    }
                } catch(XccException e) {
                    // don't retry this.  We simply log the failure
                    logger.logException("error on setting placekeys: " +
                                        _outputUri[i], e);
                }
            }

            // create the content
            Content content =
                ContentFactory.newContent(_outputUri[i],
                                          _contentBytes[i],
                                          options);

            contentList.add(content);
        }

        Content contentArray[] = contentList.toArray(new Content[0]);

        int retries = maxRetries;
        long sleepMillis = 250;
        // in case the server is unreliable, we try again N times
        // the sleep time doubles after every retry
        while (retries > 0) {
            try {
                if (configuration.useMultiStmtTxn()) {
                    session.setTransactionMode(com.marklogic.xcc.Session.TransactionMode.UPDATE);
                    for (int i = 0; i < contentArray.length; i++)
                        session.insertContent(contentArray[i]);
                    session.commit();
                    session.setTransactionMode(com.marklogic.xcc.Session.TransactionMode.QUERY);
                } else {
                    session.insertContent(contentArray);
                }

                // handle prop:properties node, optional
                // TODO do this in the same transaction
                if (copyProperties) {
                    for (int i = 0; i < _outputUri.length; i++) {
                        if (ignoreList[i])
                            continue;

                        String properties = _metadata[i].getProperties();
                        if (null != properties) {
                            try {
                                System.out.println(">>> setting properties for " + _outputUri[i]);
                                session.setDocumentProperties(_outputUri[i], properties);
                            } catch (Exception e) {
                                logger.logException("exception when setting properties", e);
                            }
                        }
                    }
                }
                // success - will not loop again
                break;
            } catch (XccException e) {
                retries--;
                if (retries > 0)
                    logger.warning("error writing document (" + _outputUri[0] + "), will retry " +
                                   retries + " more times.");
                else {
                    throw new SyncException("write failed, all retries exhausted for " + _outputUri[0], e);
                }
                sleepMillis = sleepForRetry(sleepMillis);
            }
        }

        // verify hash value
        if (configuration.useChecksumModule()) {
            try {
                String q = getQuery(_outputUri.length);
                logger.fine("writer hash query = \n" + q);
                Request req = session.newAdhocQuery(query);
                for (int i = 0; i < _outputUri.length; i++)
                    req.setNewStringVariable("URI-" + i, _outputUri[i] == null ? "" : _outputUri[i]);
                ResultSequence rs = session.submitRequest(req);
                ResultItem items[] = rs.toResultItemArray();

                for (int i = 0; i < _outputUri.length; i++) {
                    if (ignoreList[i])
                        continue;

                    String srcHash = _metadata[i].getHashValue();
                    String dstHash = items[i].asString();
                    if ((srcHash == null && dstHash != null) ||
                        !srcHash.equals(dstHash))
                        logger.warning("hash value mismatch, uri = " + _outputUri[i] +
                                       ",src hash = " + srcHash +
                                       ",dst hash = " + dstHash);
                }
            } catch (Exception e) {
                logger.logException("hash comparison failed", e);
                for (int i = 0; i < _outputUri.length; i++)
                    logger.warning("no hash comparison for uri=" + _outputUri[i]);
            }
        }

        // compute total ingested bytes
        if (retries >= 0) {
            for (int i = 0; i < _outputUri.length; i++) {
                if (ignoreList[i])
                    continue;
                bytes += _contentBytes[i].length;
            }
        }

        session.close();

        return bytes;
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
        // cap sleepMillis at 60 sec
        return (sleepMillis < 60 * 1000) ? (2 * sleepMillis) : sleepMillis;
    }

    /**
     * @param _outputUri
     * @param _metadata
     * @return
     */
    private boolean matchesFilters(String _outputUri,
            MetadataInterface _metadata) {
        // check format - return true if any filter matches
        if (null != outputFormatFilters &&
            Arrays.binarySearch(outputFormatFilters,
                                _metadata.getFormatName()) > -1) {
            logger.finer(Configuration.OUTPUT_FILTER_FORMATS_KEY
                    + " matched " + _outputUri);
            return true;
        }

        return false;
    }

    protected String getQuery(int uriCount) {
        if (query == null || uriCount != last_batch_size) {
            String local_q = "";
            String m = configuration.getChecksumModule();

            for (int i = 0; i < uriCount; i++)
                local_q += "declare variable $URI-" + i + " external;\n";

            local_q += "\n";

            for (int i = 0; i < uriCount; i++) {
                local_q += "xdmp:invoke(\"" + m + "\", (xs:QName(\"URI\"), $URI-" + i + "))\n";
                if (i < uriCount - 1)
                    local_q += ",\n";
            }

            query = local_q;
            last_batch_size = uriCount;
        }
        return query;
    }

}
