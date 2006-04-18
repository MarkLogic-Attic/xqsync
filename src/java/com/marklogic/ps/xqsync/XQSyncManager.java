/*
 * Copyright 2005-2006 Mark Logic Corporation. All rights reserved.
 *
 */
package com.marklogic.ps.xqsync;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import com.marklogic.ps.Connection;
import com.marklogic.ps.FileFinder;
import com.marklogic.ps.PropertyClientInterface;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.xdbc.XDBCException;
import com.marklogic.xdbc.XDBCResultSequence;
import com.marklogic.xdbc.XDBCXName;
import com.marklogic.xdmp.XDMPPermission;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class XQSyncManager extends Thread implements PropertyClientInterface {

    private static final String START_POSITION_PREDICATE = "[position() ge $start]\n";

    private static final String START_POSITION_DEFINE_VARIABLE = "define variable $start as xs:integer external\n";

    public static final String NAME = XQSyncManager.class.getName();

    private static SimpleLogger logger;

    private Properties properties;

    private Connection inputConnection;

    private boolean fatalErrors = true;

    private Connection outputConn;

    private String outputPath;

    private Collection readRoles;

    List documentList = new LinkedList();

    private long count;

    private int listSize;

    private String[] placeKeys = null;

    private XQSyncPackage outputPackage = null;

    private XQSyncPackage inputPackage = null;

    private boolean copyPermissions = true;

    private boolean copyProperties = true;

    private String inputPath;

    private Long startPosition;

    private boolean hasStart = false;

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
         * collection mode if property XDBC_COLLECTION is not null open xdbc
         * conneciton and list collection uris sync every uri to the OUTPUT_PATH
         * 
         * directory mode if property XDBC_ROOT_DIRECTORY is not null open xdbc
         * connection and list contents of XDBC_ROOT_DIRECTORY walk the tree,
         * synchronizing as we go sync every uri to the OUTPUT_PATH
         * 
         * default database mode open xdbc connection and list input() uris sync
         * every uri to the OUTPUT_PATH
         * 
         * if OUTPUT_PATH seems to be an xdbc "url", output to remote connection
         * 
         */
        if (properties == null || !properties.keys().hasMoreElements()) {
            logger.severe("null or empty properties");
            return;
        }

        // figure out the input source
        String inputPackagePath = properties.getProperty("INPUT_PACKAGE");

        String inputConnectionString = null;
        if (inputPackagePath == null) {
            inputPath = properties.getProperty("INPUT_PATH");
            if (inputPath != null) {
                logger.info("input from path: " + inputPath);
            } else {
                inputConnectionString = properties
                        .getProperty("INPUT_CONNECTION_STRING");
                if (inputConnectionString == null) {
                    logger
                            .severe("missing required property: INPUT_CONNECTION_STRING");
                    return;
                }
                logger.info("input from connection: " + inputConnectionString);
            }
        } else {
            logger.info("input from package: " + inputPackagePath);
        }

        String outputConnectionString = properties
                .getProperty("OUTPUT_CONNECTION_STRING");

        outputPath = properties.getProperty("OUTPUT_PATH");
        if (outputPath != null)
            outputPath = outputPath.trim();
        if (outputPath == null && outputConnectionString == null) {
            logger
                    .severe("missing required properties: OUTPUT_PATH and/or OUTPUT_CONNECTION_STRING");
            return;
        }

        try {
            if (inputConnectionString != null)
                inputConnection = new Connection(inputConnectionString);

            if (outputConnectionString != null) {
                outputConn = new Connection(outputConnectionString);
                logger.info("output to connection: " + outputConnectionString);
            } else {
                // synching to filesystem: directory or package?
                String packagePath = properties.getProperty("OUTPUT_PACKAGE");
                if (packagePath != null) {
                    outputPackage = new XQSyncPackage(new FileOutputStream(
                            packagePath));
                    logger.info("writing to package: " + packagePath);
                } else {
                    // not a zip file
                    File outputFile = new File(outputPath);
                    ensureFileExists(outputFile);
                    outputPath = outputFile.getCanonicalPath();
                }
            }

            if (inputConnection != null) {
                listDocuments();
            } else if (inputPackagePath != null) {
                listPackage(inputPackagePath);
            } else {
                listFilesystemPath(inputPath);
            }

            // remember the initial size
            listSize = documentList.size();
            logger.info("synchronizing " + listSize + " documents");

            // start your engines...
            runWorkerThreads();

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
     * @throws XDBCException
     * 
     */
    private void runWorkerThreads() throws XDBCException {
        int threads = new Integer(properties.getProperty("THREADS", "1"))
                .intValue();

        XQSyncWorker[] workerThreads = new XQSyncWorker[threads];
        for (int i = 0; i < threads; i++) {
            workerThreads[i] = new XQSyncWorker(this);
            workerThreads[i].start();
        }

        logger.info("started " + threads + " threads");
        for (int i = 0; i < threads; i++) {
            try {
                workerThreads[i].join();
            } catch (InterruptedException e) {
                logger.logException("thread " + i, e);
            }
        }
    }

    /**
     * @throws IOException
     * 
     */
    private void listPackage(String _path) throws IOException {
        // list contents of package
        logger.info("listing package " + _path);

        inputPackage = new XQSyncPackage(_path);
        documentList = inputPackage.list();
    }

    /**
     * @throws Exception
     */
    private void listDocuments() throws Exception {
        String xdbcCollection = properties.getProperty("XDBC_COLLECTION");
        String xdbcRootDirectory = properties
                .getProperty("XDBC_ROOT_DIRECTORY");
        String xdbcQuery = properties.getProperty("XDBC_QUERY");
        if (xdbcCollection != null && xdbcRootDirectory != null) {
            logger
                    .warning("conflicting properties: using XDBC_COLLECTION, not XDBC_ROOT_DIRECTORY");
        }

        startPosition = null;
        String startPositionString = properties
                .getProperty("INPUT_START_POSITION");
        if (startPositionString != null)
            startPosition = new Long(startPositionString);

        hasStart = (startPosition != null && startPosition.longValue() > 1);
        if (hasStart) {
            logger.info("using INPUT_START_POSITION="
                    + startPosition.longValue());
        }

        if (xdbcCollection != null) {
            listCollection(xdbcCollection, hasStart);

            // if requested, delete the collection
            if (Utilities.stringToBoolean(properties.getProperty(
                    "DELETE_COLLECTION", "false"))) {
                logger.info("deleting collection " + xdbcCollection
                        + " on output connection");
                outputConn.deleteCollection(xdbcCollection);
            }
        } else if (xdbcRootDirectory != null)
            listDirectory(xdbcRootDirectory, hasStart);
        else if (xdbcQuery != null)
            listQuery(xdbcQuery);
        else
            listInput(hasStart);
    }

    /**
     * @param query
     * @param externs
     * @return
     * @throws XDBCException
     */
    private LinkedList listDocuments(String query, Map externs)
            throws XDBCException {
        LinkedList list = new LinkedList();
        XDBCResultSequence rs = null;
        try {
            // it's ok if externs are null
            rs = inputConnection.executeQuery(query, externs);

            while (rs.hasNext()) {
                rs.next();
                list.add(rs.getAnyURI().asString());
            }
            rs.close();
        } finally {
            if (rs != null && !rs.isClosed())
                rs.close();
        }

        return list;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.PropertyClientInterface#setProperties(java.util.Properties)
     */
    public void setProperties(Properties _properties) {
        properties = _properties;
        logger.setProperties(_properties);

        // fatalErrors is hot
        fatalErrors = Utilities.stringToBoolean(properties.getProperty(
                "FATAL_ERRORS", "true"), true);

        // read-roles are hot
        String readRolesString = properties
                .getProperty("READ_PERMISSION_ROLES");
        if (readRolesString != null) {
            logger.fine("read roles are: " + readRolesString);
            String[] roleNames = readRolesString.split("[,;\\s]+");
            if (roleNames.length > 0) {
                readRoles = new Vector();
                for (int i = 0; i < roleNames.length; i++) {
                    logger.fine("adding read role: " + roleNames[i]);
                    readRoles.add(new XDMPPermission(XDMPPermission.READ,
                            roleNames[i]));
                }
            }
        }

        // copyPermissions is hot
        copyPermissions = Utilities.stringToBoolean(properties.getProperty(
                "COPY_PERMISSIONS", "true"));

        // copyProperties is hot
        copyPermissions = Utilities.stringToBoolean(properties.getProperty(
                "COPY_PROPERTIES", "true"));

        // placeKeys are hot
        String placeKeysString = properties.getProperty("OUTPUT_FORESTS");
        if (placeKeysString != null)
            placeKeys = placeKeysString.split(",");

    }

    /**
     * @param xdbcCollection
     * @param hasStart
     */
    private void listCollection(String xdbcCollection, boolean hasStart)
            throws Exception {
        logger.info("listing collection " + xdbcCollection);
        String query = "define variable $uri as xs:string external\n"
                + (hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                + "for $i in collection($uri)\n"
                + (hasStart ? START_POSITION_PREDICATE : "")
                + "return base-uri($i)";
        Map externs = new Hashtable(1);
        externs.put(new XDBCXName("", "uri"), xdbcCollection);
        if (hasStart)
            externs.put(new XDBCXName("", "start"), startPosition);
        documentList = listDocuments(query, externs);
    }

    /**
     * @param xdbcRootDirectory
     * @param hasStart
     * @throws Exception
     */
    private void listDirectory(String xdbcRootDirectory, boolean hasStart)
            throws Exception {
        logger.info("listing directory " + xdbcRootDirectory);
        String query = "define variable $uri as xs:string external\n"
                + (hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                + "for $i in xdmp:directory($uri, 'infinity')\n"
                + (hasStart ? START_POSITION_PREDICATE : "")
                + "return base-uri($i)";
        Map externs = new Hashtable(1);
        externs.put(new XDBCXName("", "uri"), xdbcRootDirectory);
        if (hasStart)
            externs.put(new XDBCXName("", "start"), startPosition);
        documentList = listDocuments(query, externs);
    }

    /**
     * @param hasStart
     * @throws XDBCException
     * 
     */
    private void listInput(boolean hasStart) throws Exception {
        // list all the documents in the database
        logger.info("listing all documents");
        String query = (hasStart ? START_POSITION_DEFINE_VARIABLE : "")
                + "for $i in doc()\n"
                + (hasStart ? START_POSITION_PREDICATE : "")
                + "return base-uri($i)";
        Map externs = null;
        if (hasStart) {
            externs = new Hashtable(1);
            externs.put(new XDBCXName("", "start"), startPosition);
        }
        documentList = listDocuments(query, externs);
    }

    /**
     * @param xdbcQuery
     */
    private void listQuery(String xdbcQuery) throws Exception {
        // set list of uris via a user-supplied query
        logger.info("listing query: " + xdbcQuery);
        documentList = listDocuments(xdbcQuery, null);
    }

    /**
     * @param _inputPath
     * @throws IOException
     */
    private void listFilesystemPath(String _inputPath) throws IOException {
        // build documentList from a filesystem path
        // exclude stuff that ends with '.metadata'
        logger.info("listing from " + _inputPath + ", excluding "
                + XQSyncDocument.METADATA_REGEX);
        FileFinder ff = new FileFinder(_inputPath, null,
                XQSyncDocument.METADATA_REGEX);
        ff.find();
        documentList = ff.listRelativePaths(_inputPath);
    }

    /**
     * @param requested
     * @return
     */
    public synchronized String[] getUri(int requested) {
        if (documentList.size() < 1)
            return null;

        if (requested == 1)
            return new String[] { getUri() };

        LinkedList list = new LinkedList();
        String uri;
        while (documentList.size() > 0 && list.size() < requested) {
            count++;
            uri = (String) documentList.remove(0);
            list.add(uri);
            logProgress(uri);
        }

        return (String[]) list.toArray(new String[0]);
    }

    /**
     * @return
     */
    public synchronized String getUri() {
        if (documentList.size() < 1)
            return null;

        count++;
        String uri = (String) documentList.remove(0);
        logProgress(uri);
        return uri;
    }

    private void logProgress(String uri) {
        long start = hasStart ? startPosition.longValue() : 0;
        logger.info("synchronizing " + (count + start) + " of "
                + (listSize + start) + ": " + uri);
    }

    /**
     * @return
     */
    public SimpleLogger getLogger() {
        return logger;
    }

    /**
     * @return
     */
    public Connection getInputConnection() {
        return inputConnection;
    }

    /**
     * @return
     */
    public Connection getOutputConnection() {
        return outputConn;
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
    public Collection getReadRoles() {
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

}