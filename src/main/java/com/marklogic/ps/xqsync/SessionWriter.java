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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.math.BigInteger;

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
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class SessionWriter extends AbstractWriter {

    protected static Object firstMaxTasksMutex = new Object();
    protected static boolean firstMaxTasks = false;
    protected Map<String, BigInteger> forestMap = null;
    protected String[] forestNameArray = null;
    protected int lastBatchSize = -1;
    protected String query = null;
    protected int maxRetries = 3;

   /**
     * @param configuration
     * @throws SyncException
     */
    public SessionWriter(Configuration configuration)
            throws SyncException {
        // superclass takes care of some configuration
        super(configuration);

        // prepare for in-forest eval
        if (configuration.useInForestEval()) {
            forestMap = configuration.getOutputForestMap();
            if (forestMap == null) {
                throw new SyncException("cannot retrieve forest map");
            }
            forestNameArray = placeKeys;
            if (forestNameArray == null || forestNameArray.length == 0) {
                forestNameArray = configuration.getOutputForestNames();
            }
        }

        maxRetries = configuration.getMaxRetries();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.ps.xqsync.WriterInterface#write(java.lang.String,
     * byte[], com.marklogic.ps.xqsync.XQSyncDocumentMetadata)
     */
    public int write(String outputUri, byte[] contentBytes, XQSyncDocumentMetadata metadata) throws SyncException {
        String[] outputUriArray = new String[1];
        byte[][] contentBytesArray = new byte[1][0];
        XQSyncDocumentMetadata[] metadataArray = new XQSyncDocumentMetadata[1];
        outputUriArray[0] = outputUri;
        contentBytesArray[0] = contentBytes;
        metadataArray[0] = metadata;
        return write(outputUriArray, contentBytesArray, metadataArray);
    }

    /**
     * This version writes multiple documents in a single txn.
     *
     * @param outputUri
     * @param contentBytes
     * @param metadata
     * @return
     *
     * returns the number of Bytes written
     * @throws SyncException
     */
    @Override
    public int write(String[] outputUri, byte[][] contentBytes, XQSyncDocumentMetadata[] metadata) throws SyncException {
        int bytes = 0;
        boolean useInForestEval = configuration.useInForestEval();

        // do nothing if there's no input
        if (null == outputUri || outputUri.length == 0) {
            return bytes;
        }
        // check which ones we really need to process by running
        // everything through filtes
        boolean[] ignoreList = new boolean[outputUri.length];
        for (int i = 0; i < outputUri.length; i++) {
            ignoreList[i] = matchesFilters(outputUri[i], metadata[i]);
        }
        // ignore the ones that have no uri
        for (int i = 0; i < outputUri.length; i++) {
            if (!ignoreList[i] && (null == outputUri[i] || outputUri[i].isEmpty())) {
                ignoreList[i] = true;
            }
        }
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
            int evalForestIdx = Math.abs(outputUri[0].hashCode() % forestNameArray.length);
            forestName = forestNameArray[evalForestIdx];
            forestIdBigInt = forestMap.get(forestName);
            if (null == forestIdBigInt) {
                throw new FatalException("forest " + forestName + " not found");
            }
            session = configuration.newOutputSession("#"+ forestIdBigInt);
        }

        if (null == session) {
            throw new FatalException("null output session");
        }

        // handle delete requests.  These are URIs that has 0
        // contentBytes
        if (!useInForestEval) {
            for (int i = 0; i < outputUri.length; i++) {
                if (!ignoreList[i] && (null == contentBytes[i] || contentBytes[i].length < 1)) {
                    ignoreList[i] = true;
                    try {
                        session.deleteDocument(outputUri[i]);
                    } catch (XccException e) {
                        // don't retry delete.  We simply log the failure
                        logger.logException("error deleting document: " + outputUri[i], e);
                    }
                }
            }

            // skip existing documents if requested
            if (skipExisting) {
                for (int i = 0; i < outputUri.length; i++) {
                    if (!ignoreList[i]) {
                        try {
                            if (session.existsDocument(outputUri[i])) {
                                ignoreList[i] = true;
                            }
                        } catch (XccException e) {
                            // don't retry this.  We simply log the failure
                            logger.logException("error on check existing document: " + outputUri[i], e);
                        }
                    }
                }
            }
        }

        // create the contents to be inserted
        List<Content> contentList = new ArrayList<>(outputUri.length);
        for (int i = 0; i < outputUri.length; i++) {
            if (ignoreList[i]) {
                continue;
            }
            ContentCreateOptions options = null;
            if (metadata[i].isBinary()) {
                logger.fine(outputUri[i] + " is binary");
                options = ContentCreateOptions.newBinaryInstance();
            } else if (metadata[i].isText()) {
                logger.fine(outputUri[i] + " is text");
                options = ContentCreateOptions.newTextInstance();
            } else {
                logger.fine(outputUri[i] + " is xml");
                options = ContentCreateOptions.newXmlInstance();
            }

            // permissions
            metadata[i].addPermissions(permissionRoles);
            ContentPermission[] permissions = metadata[i].getPermissions();
            if (permissions.length > 0) {
                options.setPermissions(permissions);
            }
            // collections
            String[] collections = metadata[i].getCollections();
            logger.fine("collections = " + Utilities.join(collections, " "));
            options.setCollections(collections);

            // quality
            options.setQuality(metadata[i].getQuality());

            // namespace, seems to be always null
            options.setNamespace(null);

            // repair level
            DocumentRepairLevel repair = (!repairInputXml) ? DocumentRepairLevel.NONE : DocumentRepairLevel.FULL;
            logger.fine("repair = " + repairInputXml + ", " + repair);
            options.setRepairLevel(repair);

            // place keys
            if (null != placeKeys) {
                try {
                    if (forestIdBigInt == null) {
                        logger.finest("placeKeys = " + Utilities.join(placeKeys, ","));
                        options.setPlaceKeys(session.forestNamesToIds(placeKeys));
                    } else {
                        BigInteger[] forestIds = {forestIdBigInt};
                        options.setPlaceKeys(forestIds);
                    }
                } catch(XccException e) {
                    // don't retry this.  We simply log the failure
                    logger.logException("error on setting placekeys: " + outputUri[i], e);
                }
            }

            // create the content
            Content content = ContentFactory.newContent(outputUri[i], contentBytes[i], options);
            contentList.add(content);
        }

        Content[] contentArray = contentList.toArray(new Content[0]);

        int retries = maxRetries;
        long sleepMillis = 250;
        // in case the server is unreliable, we try again N times
        // the sleep time doubles after every retry
        while (retries > 0) {
            try {
                if (configuration.useMultiStmtTxn()) {
                    session.setTransactionMode(com.marklogic.xcc.Session.TransactionMode.UPDATE);
                    for (Content content : contentArray) {
                        session.insertContent(content);
                    }
                    session.commit();
                    session.setTransactionMode(com.marklogic.xcc.Session.TransactionMode.QUERY);
                } else {
                    session.insertContent(contentArray);
                }

                // handle prop:properties node, optional
                // TODO do this in the same transaction
                if (copyProperties) {
                    for (int i = 0; i < outputUri.length; i++) {
                        if (ignoreList[i]) {
                            continue;
                        }
                        String properties = metadata[i].getProperties();
                        if (null != properties) {
                            try {
                                System.out.println(">>> setting properties for " + outputUri[i]);
                                session.setDocumentProperties(outputUri[i], properties);
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
                if (retries > 0) {
                    logger.warning("error writing document (" + outputUri[0] + "), will retry " + retries + " more times.");
                } else {
                    throw new SyncException("write failed, all retries exhausted for " + outputUri[0], e);
                }
                sleepMillis = sleepForRetry(sleepMillis);
            }
        }

        // verify hash value
        if (configuration.useChecksumModule()) {
            try {
                String q = getQuery(outputUri.length);
                logger.fine("writer hash query = \n" + q);
                Request req = session.newAdhocQuery(query);
                for (int i = 0; i < outputUri.length; i++) {
                    req.setNewStringVariable("URI-" + i, outputUri[i] == null ? "" : outputUri[i]);
                }
                ResultSequence rs = session.submitRequest(req);
                ResultItem[] items = rs.toResultItemArray();

                for (int i = 0; i < outputUri.length; i++) {
                    if (ignoreList[i]) {
                        continue;
                    }
                    String srcHash = metadata[i].getHashValue();
                    String dstHash = items[i].asString();
                    if ((srcHash == null && dstHash != null) ||
                        !srcHash.equals(dstHash)) {
                        logger.warning("hash value mismatch, uri = " + outputUri[i] +
                            ",src hash = " + srcHash +
                            ",dst hash = " + dstHash);
                    }
                }
            } catch (Exception e) {
                logger.logException("hash comparison failed", e);
                for (String s : outputUri) { logger.warning("no hash comparison for uri=" + s); }
            }
        }

        // compute total ingested bytes
        if (retries >= 0) {
            for (int i = 0; i < outputUri.length; i++) {
                if (ignoreList[i]) {
                    continue;
                }
                bytes += contentBytes[i].length;
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
            logger.logException("interrupted during sleep " + sleepMillis, e);
        }
        // cap sleepMillis at 60 sec
        return (sleepMillis < 60 * 1000) ? (2 * sleepMillis) : sleepMillis;
    }

    /**
     * @param outputUri
     * @param metadata
     * @return
     */
    private boolean matchesFilters(String outputUri, MetadataInterface metadata) {
        // check format - return true if any filter matches
        if (null != outputFormatFilters && Arrays.binarySearch(outputFormatFilters, metadata.getFormatName()) > -1) {
            logger.finer(Configuration.OUTPUT_FILTER_FORMATS_KEY + " matched " + outputUri);
            return true;
        }
        return false;
    }

    protected String getQuery(int uriCount) {
        if (query == null || uriCount != lastBatchSize) {
            StringBuilder localQuery = new StringBuilder();
            String m = configuration.getChecksumModule();

            for (int i = 0; i < uriCount; i++) {
                localQuery.append("declare variable $URI-").append(i).append(" external;\n");
            }
            localQuery.append("\n");

            for (int i = 0; i < uriCount; i++) {
                localQuery.append("xdmp:invoke(\"").append(m).append("\", (xs:QName(\"URI\"), $URI-").append(i).append("))\n");
                if (i < uriCount - 1) {
                    localQuery.append(",\n");
                }
            }

            query = localQuery.toString();
            lastBatchSize = uriCount;
        }
        return query;
    }

}
