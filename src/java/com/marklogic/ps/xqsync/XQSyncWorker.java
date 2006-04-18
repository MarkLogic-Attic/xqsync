/*
 * Copyright (c)2004-2006 Mark Logic Corporation
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

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import com.marklogic.ps.Connection;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.xdbc.XDBCDatabaseMetaData;
import com.marklogic.xdbc.XDBCException;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 *
 */
public class XQSyncWorker extends Thread {

    public static final String NAME = XQSyncWorker.class.getName();

    private static SimpleLogger logger;

    private Connection inputConnection = null;

    private boolean fatalErrors = true;

    private Connection outputConn = null;

    private String outputPath = null;

    private int connMajorVersion = -1;

    private Collection readRoles = null;

    private XQSyncManager manager = null;

    private boolean keepRunning = true;

    private String[] placeKeys = null;

    private XQSyncPackage outputPackage = null;

    private XQSyncPackage inputPackage;

    private String inputPath = null;

    private int BLOCK_SIZE = 1;

    private boolean copyPermissions = true;

    private boolean copyProperties = true;

    /**
     * @param _properties
     * @throws XDBCException
     */
    public XQSyncWorker(XQSyncManager _manager) throws XDBCException {
        manager = _manager;

        logger = manager.getLogger();

        inputPackage = manager.getInputPackage();
        if (inputPackage == null) {
            inputPath = manager.getInputPath();
            if (inputPath == null) {
                inputConnection = manager.getInputConnection();
            }
        }

        outputConn = manager.getOutputConnection();
        outputPath = manager.getOutputPath();
        outputPackage = manager.getOutputPackage();

        readRoles = manager.getReadRoles();
        fatalErrors = manager.getFatalErrors();
        copyPermissions = manager.getCopyPermissions();
        copyProperties = manager.getCopyProperties();
        placeKeys = manager.getPlaceKeys();
    }

    public void run() {
        // check for required fields
        if (inputPackage == null && inputPath == null
                && inputConnection == null) {
            logger
                    .severe("missing inputConnection, inputPaths, and inputPackage");
            return;
        }

        if (outputPath == null && outputConn == null) {
            logger.severe("missing outputPath and outputConnection");
            return;
        }

        // read from the queue and write to output
        try {
            if (outputConn != null) {
                logger.finest("synching to remote server: "
                        + outputConn.getConnectionString());
            } else {
                // synching to filesystem
                File outputFile = new File(outputPath);
                if (outputPackage == null) {
                    if (!(outputFile.exists() && outputFile.canRead() && outputFile
                            .canWrite()))
                        throw new IOException(
                                "OUTPUT_PATH is missing or cannot be read: "
                                        + outputPath);
                    outputPath = outputFile.getCanonicalPath();
                }
            }

            if (inputConnection != null) {
                XDBCDatabaseMetaData metadata = inputConnection.getConnection()
                        .getMetaData();
                connMajorVersion = metadata.getDBMajorVersion();
                logger.finer("input connection database is version "
                        + connMajorVersion);
            }

            String[] uris;
            String uri;
            while (keepRunning) {
                try {
                    uris = manager.getUri(BLOCK_SIZE);

                    if (uris == null || uris.length < 1) {
                        logger
                                .finer("nothing available from manager: stopping");
                        keepRunning = false;
                        break;
                    }

                    // TODO push the array down,
                    // to build several documents at once
                    // this may improve performance
                    for (int i = 0; i < uris.length; i++) {
                        uri = uris[i];
                        try {
                            syncDocument(uri);
                        } catch (Exception e) {
                            logger.logException(uri, e);
                            // if fatalErrors is set, halt processing
                            if (fatalErrors)
                                throw e;
                        }
                    }
                } catch (Exception e) {
                    if (fatalErrors)
                        throw e;
                    else
                        logger.logException("continuing despite error", e);
                }
            }

        } catch (Exception e) {
            logger.logException("run-level exception", e);
            if (fatalErrors)
                System.exit(-1);
        }

        logger.fine("exiting");
    }

    /**
     * @param uri
     * @throws Exception
     */
    protected void syncDocument(String uri) throws Exception {
        /*
         * handle various possibilities: to-from database, package, filesystem
         */

        XQSyncDocument doc;

        // marshal input as package entry
        if (inputConnection != null) {
            doc = new XQSyncDocument(uri, inputConnection, copyPermissions,
                    copyProperties, connMajorVersion);
        } else if (inputPackage != null) {
            doc = new XQSyncDocument(uri, inputPackage, copyPermissions,
                    copyProperties);
        } else if (inputPath != null) {
            doc = new XQSyncDocument(uri, inputPath, copyPermissions,
                    copyProperties);
        } else {
            throw new IOException("must define inputConnection or inputPackage");
        }
        // provide a logger to the document
        doc.setLogger(logger);

        // marshal output arguments
        // build remote URI from outputPath and uri
        String path = outputPath;
        // path may be empty: if not, it should end with separator
        if (path != null && !path.equals("")) {
            path += "/";
        } else {
            path = "";
        }
        // ensure exactly one separator at each level
        String outputURI = (path + uri).replaceAll("//+", "/");
        logger.fine("copying " + uri + " to " + outputURI);

        // output
        if (outputConn != null) {
            doc.write(outputURI, outputConn, readRoles, placeKeys);
        } else if (outputPackage != null) {
            doc.write(outputURI, outputPackage, readRoles);
        } else if (outputPath != null) {
            doc.write(outputURI, readRoles);
        } else {
            throw new IOException(
                    "must define outputConnection, outputPackage, or outputPath");
        }
    }

    public void stopRunning() {
        keepRunning = false;
    }

}
