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
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.marklogic.ps.Connection;
import com.marklogic.ps.FileFinder;
import com.marklogic.ps.PropertyClientInterface;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class XQSyncManager extends Thread implements
        PropertyClientInterface {

    /**
     * 
     */
    private static final String THREADS_DEFAULT = "1";

    /**
     * 
     */
    private static final String THREADS_KEY = "THREADS";

    /**
     * 
     */
    private static final String XCC_PREFIX = "xcc://";

    /**
     * 
     */
    private static final String XCC_PREFIX_OLD = "xdbc://";

    /**
     * 
     */
    private static final String OUTPUT_FORESTS_KEY = "OUTPUT_FORESTS";

    /**
     * 
     */
    private static final String COPY_PROPERTIES_KEY = "COPY_PROPERTIES";

    /**
     * 
     */
    private static final String COPY_PERMISSIONS_KEY = "COPY_PERMISSIONS";

    /**
     * 
     */
    private static final String READ_PERMISSION_ROLES_KEY = "READ_PERMISSION_ROLES";

    /**
     * 
     */
    private static final String FATAL_ERRORS_KEY = "FATAL_ERRORS";

    /**
     * 
     */
    private static final String DELETE_COLLECTION_KEY = "DELETE_COLLECTION";

    /**
     * 
     */
    public static final String INPUT_START_POSITION_KEY = "INPUT_START_POSITION";

    /**
     * 
     */
    public static final String INPUT_QUERY_KEY = "INPUT_QUERY";

    /**
     * 
     */
    public static final String INPUT_DOCUMENT_URIS_KEY = "INPUT_DOCUMENT_URIS";

    /**
     * 
     */
    public static final String OUTPUT_PACKAGE_KEY = "OUTPUT_PACKAGE";

    /**
     * 
     */
    public static final String OUTPUT_CONNECTION_STRING_KEY = "OUTPUT_CONNECTION_STRING";

    /**
     * 
     */
    public static final String OUTPUT_PATH_KEY = "OUTPUT_PATH";

    /**
     * 
     */
    public static final String INPUT_CONNECTION_STRING_KEY = "INPUT_CONNECTION_STRING";

    /**
     * 
     */
    public static final String INPUT_PATH_KEY = "INPUT_PATH";

    /**
     * 
     */
    public static final String INPUT_PACKAGE_KEY = "INPUT_PACKAGE";

    public static final String INPUT_DIRECTORY_URI = "INPUT_DIRECTORY_URI";

    public static final String INPUT_COLLECTION_URI_KEY = "INPUT_COLLECTION_URI";

    private static final String START_VARIABLE_NAME = "start";

    private static final String START_POSITION_PREDICATE = "[position() ge $start]\n";

    private static final String START_POSITION_DEFINE_VARIABLE = "define variable $start as xs:integer external\n";

    public static final String NAME = XQSyncManager.class.getName();

    private static SimpleLogger logger;

    private Properties properties;

    private Connection inputConnection;

    private boolean fatalErrors = true;

    private Connection outputConnection;

    private String outputPath;

    private Collection<ContentPermission> readRoles;

    private long count;

    private String[] placeKeys = null;

    private XQSyncPackage outputPackage = null;

    private XQSyncPackage inputPackage = null;

    private boolean copyPermissions = true;

    private boolean copyProperties = true;

    private String inputPath;

    private Long startPosition;

    private boolean hasStart = false;

    private com.marklogic.ps.Session outputSession;

    private com.marklogic.ps.Session inputSession;

    private TaskFactory factory;

    private Monitor monitor;

    /**
     * @param _properties
     */
    public XQSyncManager(SimpleLogger _logger, Properties _properties) {
        logger = _logger;
        setProperties(_properties);
    }

    public void run() {
        /*
         * multiple run modes:
         * 
         * collection mode: if property INPUT_COLLECTION_URI is not null, open
         * input connection and list collection uris sync every uri to the
         * OUTPUT_PATH or OUTPUT_CONNECTION_STRING.
         * 
         * directory mode: if property INPUT_DIRECTORY_URI is not null, open
         * input connection and list contents of directory - walk the tree,
         * synchronizing as we go sync every uri to the OUTPUT_PATH or
         * OUTPUT_CONNECTION_STRING.
         * 
         * (default) database mode: open input connection and list doc() uris,
         * synchronizing every uri to the OUTPUT_PATH or
         * OUTPUT_CONNECTION_STRING.
         * 
         */
        if (properties == null || !properties.keys().hasMoreElements()) {
            logger.severe("null or empty properties");
            return;
        }

        // figure out the input source
        String inputPackagePath = properties
                .getProperty(INPUT_PACKAGE_KEY);

        String inputConnectionString = null;
        URI inputUri = null;
        if (inputPackagePath == null) {
            inputPath = properties.getProperty(INPUT_PATH_KEY);
            if (inputPath != null) {
                logger.info("input from path: " + inputPath);
            } else {
                inputConnectionString = properties
                        .getProperty(INPUT_CONNECTION_STRING_KEY);
                if (inputConnectionString == null) {
                    logger.severe("missing required property: "
                            + INPUT_CONNECTION_STRING_KEY);
                    return;
                }
                if (!(inputConnectionString.startsWith(XCC_PREFIX) || inputConnectionString
                        .startsWith(XCC_PREFIX_OLD))) {
                    logger.fine("fixing connection string "
                            + inputConnectionString);
                    inputConnectionString = XCC_PREFIX
                            + inputConnectionString;
                }
                logger.info("input from connection: "
                        + inputConnectionString);
                try {
                    inputUri = new URI(inputConnectionString);
                } catch (URISyntaxException e) {
                    logger.logException("malformed property: "
                            + inputConnectionString, e);
                    return;
                }
            }
        } else {
            logger.info("input from package: " + inputPackagePath);
        }

        String outputConnectionString = properties
                .getProperty(OUTPUT_CONNECTION_STRING_KEY);
        URI outputUri = null;
        if (outputConnectionString != null) {
            if (!(outputConnectionString.startsWith(XCC_PREFIX) || outputConnectionString
                    .startsWith(XCC_PREFIX_OLD))) {
                logger.fine("fixing connection string "
                        + outputConnectionString);
                outputConnectionString = XCC_PREFIX
                        + outputConnectionString;
            }
            try {
                outputUri = new URI(outputConnectionString);
            } catch (URISyntaxException e) {
                logger.logException("malformed property: "
                        + outputConnectionString, e);
                return;
            }
        }
        outputPath = properties.getProperty(OUTPUT_PATH_KEY);
        if (outputPath != null)
            outputPath = outputPath.trim();
        if (outputPath == null && outputConnectionString == null) {
            logger.severe("missing required properties: "
                    + OUTPUT_PATH_KEY + " and/or "
                    + OUTPUT_CONNECTION_STRING_KEY);
            return;
        }

        try {
            if (inputUri != null) {
                inputConnection = new Connection(inputUri);
                inputSession = (com.marklogic.ps.Session) inputConnection
                        .newSession();
            }

            if (outputUri != null) {
                outputConnection = new Connection(outputUri);
                outputSession = (com.marklogic.ps.Session) outputConnection
                        .newSession();
                logger.info("output to connection: "
                        + outputConnectionString);
            } else {
                // synching to filesystem: directory or package?
                String packagePath = properties
                        .getProperty(OUTPUT_PACKAGE_KEY);
                if (packagePath != null) {
                    outputPackage = new XQSyncPackage(
                            new FileOutputStream(packagePath));
                    logger.info("writing to package: " + packagePath);
                } else {
                    // not a zip file
                    File outputFile = new File(outputPath);
                    ensureFileExists(outputFile);
                    outputPath = outputFile.getCanonicalPath();
                }
            }

            // start your engines...
            int threads = new Integer(properties.getProperty(THREADS_KEY,
                    THREADS_DEFAULT)).intValue();
            logger.info("starting pool of " + threads + " threads");
            ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors
                    .newFixedThreadPool(threads);
            pool.prestartAllCoreThreads();

            ExecutorCompletionService completionService = new ExecutorCompletionService(
                    pool);

            monitor = new Monitor();
            monitor.setLogger(logger);
            monitor.setPool(pool);
            monitor.setTasks(completionService);
            monitor.start();

            factory = new TaskFactory(this);

            long itemsQueued;
            if (inputConnection != null) {
                itemsQueued = queueFromInputConnection(completionService);
            } else if (inputPackagePath != null) {
                itemsQueued = queueFromInputPackage(completionService,
                        inputPackagePath);
            } else {
                itemsQueued = queueFromInputPath(completionService, inputPath);
            }

            logger.info("synchronizing " + itemsQueued + " documents");
            pool.shutdown();
            monitor.setNumberOfTasks(itemsQueued);

            while (monitor.isAlive()) {
                try {
                monitor.join();
                } catch (InterruptedException e) {
                    logger.logException("interrupted", e);
                }
            }

            // clean up
            if (outputPackage != null) {
                try {
                    outputPackage.flush();
                    outputPackage.close();
                } catch (IOException e) {
                    logger.logException("error cleanup up package", e);
                }
            }

        } catch (Exception e) {
            logger.logException("fatal exception", e);
            if (monitor != null) {
                monitor.halt();
            }
        }

        logger.info("exiting");
    }

    /**
     * @param outputFile
     * @throws IOException
     */
    private void ensureFileExists(File outputFile) throws IOException {
        outputFile.mkdirs();
        if (!(outputFile.exists())) {
            throw new IOException(
                    "path does not exist and could not be created: "
                            + outputFile.getCanonicalPath());
        }
        if (!(outputFile.canRead())) {
            throw new IOException("path cannot be read: "
                    + outputFile.getCanonicalPath());
        }
        if (!(outputFile.canWrite())) {
            throw new IOException("path is not writable: "
                    + outputFile.getCanonicalPath());
        }
    }

    /**
     * @param completionService
     * @return
     * @throws IOException
     * @throws XccException
     * 
     */
    @SuppressWarnings("unchecked")
    private long queueFromInputPackage(ExecutorCompletionService completionService,
            String _path) throws IOException, XccException {
        // list contents of package
        logger.info("listing package " + _path);

        inputPackage = new XQSyncPackage(_path);
        factory.setInputPackage(inputPackage);

        Iterator<String> iter = inputPackage.list().iterator();
        String path;
        long count = 0;
        CallableSync ft;

        while (iter.hasNext()) {
            count++;
            path = iter.next();
            logger.finer("queuing " + count + ": " + path);
            ft = factory.newCallableSync(path);
            completionService.submit(ft);
        }

        return count;
    }

    /**
     * @param completionService
     * @throws XccException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private long queueFromInputConnection(ExecutorCompletionService completionService)
            throws XccException, IOException {
        String collectionUri = properties
                .getProperty(INPUT_COLLECTION_URI_KEY);
        String directoryUri = properties.getProperty(INPUT_DIRECTORY_URI);
        String documentUris = properties
                .getProperty(INPUT_DOCUMENT_URIS_KEY);
        String userQuery = properties.getProperty(INPUT_QUERY_KEY);
        if (collectionUri != null && directoryUri != null) {
            logger.warning("conflicting properties: using "
                    + INPUT_COLLECTION_URI_KEY + ", not "
                    + INPUT_DIRECTORY_URI);
        }

        startPosition = null;
        String startPositionString = properties
                .getProperty(INPUT_START_POSITION_KEY);
        if (startPositionString != null)
            startPosition = new Long(startPositionString);

        hasStart = (startPosition != null && startPosition.longValue() > 1);
        if (hasStart) {
            logger.info("using " + INPUT_START_POSITION_KEY + "="
                    + startPosition.longValue());
        }

        factory.setInputConnection(inputConnection);

        Request request = getRequest(collectionUri, directoryUri,
                documentUris, userQuery, hasStart);
        RequestOptions opts = request.getEffectiveOptions();
        logger.fine("buffer size = " + opts.getResultBufferSize());
        logger.fine("caching = " + opts.getCacheResult());
        opts.setCacheResult(false);
        opts.setResultBufferSize(8 * 1024);
        request.setOptions(opts);
        logger.fine("buffer size = " + opts.getResultBufferSize());
        logger.fine("caching = " + opts.getCacheResult());

        ResultSequence rs = inputSession.submitRequest(request);

        String uri;
        CallableSync ft;
        long count = 0;

        while (rs.hasNext()) {
            uri = rs.next().asString();
            logger.fine("queuing " + count + ": " + uri);
            ft = factory.newCallableSync(uri);
            completionService.submit(ft);
            count++;
        }
        rs.close();

        return count;
    }

    /**
     * @param collectionUri
     * @param directoryUri
     * @param documentUris
     * @param userQuery
     * @param hasStart
     * @return
     * @throws XccException
     */
    private Request getRequest(String collectionUri, String directoryUri,
            String documentUris, String userQuery, boolean hasStart)
            throws XccException {
        Request request;
        if (collectionUri != null) {
            request = getCollectionRequest(collectionUri);

            // if requested, delete the collection
            if (outputSession != null
                    && Utilities.stringToBoolean(properties.getProperty(
                            DELETE_COLLECTION_KEY, "false"))) {
                logger.info("deleting collection " + collectionUri
                        + " on output connection");
                outputSession.deleteCollection(collectionUri);
            }
        } else if (directoryUri != null) {
            request = getDirectoryRequest(directoryUri);
        } else if (documentUris != null) {
            request = getDocumentUrisRequest(documentUris.split("\\s+"));
        } else if (userQuery != null) {
            // set list of uris via a user-supplied query
            logger.info("listing query: " + userQuery);
            if (hasStart) {
                logger
                        .warning("ignoring start value in user-supplied query");
                hasStart = false;
            }
            request = inputSession.newAdhocQuery(userQuery);
        } else {
            // list all the documents in the database
            logger.info("listing all documents");
            String query = (hasStart ? START_POSITION_DEFINE_VARIABLE
                    : "")
                    + "for $i in doc()\n"
                    + (hasStart ? START_POSITION_PREDICATE : "")
                    + "return string(base-uri($i))";
            request = inputSession.newAdhocQuery(query);
        }

        if (hasStart) {
            request.setNewIntegerVariable(START_VARIABLE_NAME,
                    startPosition);
        }
        return request;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.PropertyClientInterface#setProperties(java.util.Properties)
     */
    public void setProperties(Properties _properties) {
        logger.setProperties(_properties);
        properties = _properties;

        // fatalErrors is hot
        fatalErrors = Utilities.stringToBoolean(properties.getProperty(
                FATAL_ERRORS_KEY, "true"), true);

        // read-roles are hot
        String readRolesString = properties
                .getProperty(READ_PERMISSION_ROLES_KEY);
        if (readRolesString != null) {
            logger.fine("read roles are: " + readRolesString);
            String[] roleNames = readRolesString.split("[,;\\s]+");
            if (roleNames.length > 0) {
                readRoles = new Vector<ContentPermission>();
                for (int i = 0; i < roleNames.length; i++) {
                    logger.fine("adding read role: " + roleNames[i]);
                    readRoles.add(new ContentPermission(
                            ContentPermission.READ, roleNames[i]));
                }
            }
        }

        // copyPermissions is hot
        copyPermissions = Utilities.stringToBoolean(properties
                .getProperty(COPY_PERMISSIONS_KEY, "true"));

        // copyProperties is hot
        copyPermissions = Utilities.stringToBoolean(properties
                .getProperty(COPY_PROPERTIES_KEY, "true"));

        // placeKeys are hot
        String placeKeysString = properties
                .getProperty(OUTPUT_FORESTS_KEY);
        if (placeKeysString != null)
            placeKeys = placeKeysString.split(",");

    }

    /**
     * @param _uri
     */
    private Request getCollectionRequest(String _uri) {
        logger.info("listing collection " + _uri);
        String query = "define variable $uri as xs:string external\n"
                + (hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                + "for $i in collection($uri)\n"
                + (hasStart ? START_POSITION_PREDICATE : "")
                + "return string(base-uri($i))\n";
        Request request = inputSession.newAdhocQuery(query);
        request.setNewStringVariable("uri", _uri);
        return request;
    }

    /**
     * @param _uris
     * @return
     */
    private Request getDocumentUrisRequest(String[] _uris) {
        String urisString = Utilities.join(_uris, " ");
        logger.info("listing documents " + urisString);
        String query = "define variable $uris-string as xs:string external\n"
                + (hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                + "for $i in doc(tokenize($uris-string, '\\s+'))\n"
                + (hasStart ? START_POSITION_PREDICATE : "")
                + "return string(base-uri($i))\n";
        Request request = inputSession.newAdhocQuery(query);
        request.setNewStringVariable("uris-string", urisString);
        return request;
    }

    /**
     * @param _uri
     * @return
     */
    private Request getDirectoryRequest(String _uri) {
        logger.info("listing directory " + _uri);
        String query = "define variable $uri as xs:string external\n"
                + (hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                + "for $i in xdmp:directory($uri, 'infinity')\n"
                + (hasStart ? START_POSITION_PREDICATE : "")
                + "return string(base-uri($i))\n";
        Request request = inputSession.newAdhocQuery(query);
        request.setNewStringVariable("uri", _uri);
        return request;
    }

    /**
     * @param completionService
     * @param _inputPath
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private long queueFromInputPath(ExecutorCompletionService completionService,
            String _inputPath) throws IOException {
        // build documentList from a filesystem path
        // exclude stuff that ends with '.metadata'
        logger.info("listing from " + _inputPath + ", excluding "
                + XQSyncDocument.METADATA_REGEX);
        FileFinder ff = new FileFinder(_inputPath, null,
                XQSyncDocument.METADATA_REGEX);
        ff.find();

        Iterator<File> iter = ff.list().iterator();
        File file;
        CallableSync ft;
        while (iter.hasNext()) {
            count++;
            file = iter.next();
            logger.finer("queuing " + count + ": "
                    + file.getCanonicalPath());
            ft = factory.newCallableSync(file);
            completionService.submit(ft);
        }

        return count;
    }

    /**
     * @return
     */
    public SimpleLogger getLogger() {
        return logger;
    }

    /**
     * @return
     * @return
     */
    public com.marklogic.ps.Session getInputConnection() {
        return inputSession;
    }

    /**
     * @return
     */
    public com.marklogic.ps.Session getOutputConnection() {
        return outputSession;
    }

    /**
     * @return
     */
    public String getOutputPath() {
        return outputPath;
    }

    /**
     * @return
     */
    public Collection<ContentPermission> getReadRoles() {
        return readRoles;
    }

    /**
     * @return
     */
    public boolean getFatalErrors() {
        return fatalErrors;
    }

    /**
     * @return
     */
    public boolean getCopyPermissions() {
        return copyPermissions;
    }

    /**
     * @return
     */
    public String[] getPlaceKeys() {
        return placeKeys;
    }

    /**
     * @return
     */
    public XQSyncPackage getOutputPackage() {
        return outputPackage;
    }

    /**
     * @return
     */
    public XQSyncPackage getInputPackage() {
        return inputPackage;
    }

    /**
     * @return
     */
    public boolean getCopyProperties() {
        return copyProperties;
    }

    /**
     * @return
     */
    public String getInputPath() {
        return inputPath;
    }

    /**
     * @return
     */
    public URI getOutputConnectionUri() {
        return outputConnection.getUri();
    }

    /**
     * @return
     */
    public Monitor getMonitor() {
        return monitor;
    }

}
