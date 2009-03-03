/*
 * Copyright (c)2004-2009 Mark Logic Corporation
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
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Properties;
import java.util.Vector;

import com.marklogic.ps.Connection;
import com.marklogic.ps.Session;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Configuration extends AbstractConfiguration {

    public static final String ALLOW_EMPTY_METADATA_KEY = "ALLOW_EMPTY_METADATA";

    public static final String ALLOW_EMPTY_METADATA_DEFAULT = "false";

    public static final String COPY_COLLECTIONS_KEY = "COPY_COLLECTIONS";

    public static final String COPY_COLLECTIONS_DEFAULT = "true";

    public static final String COPY_PROPERTIES_KEY = "COPY_PROPERTIES";

    public static final String COPY_PROPERTIES_DEFAULT = "true";

    public static final String COPY_PERMISSIONS_KEY = "COPY_PERMISSIONS";

    public static final String COPY_PERMISSIONS_DEFAULT = "true";

    public static final String COPY_QUALITY_KEY = "COPY_QUALITY";

    public static final String COPY_QUALITY_DEFAULT = "true";

    public static final String DELETE_COLLECTION_KEY = "DELETE_COLLECTION";

    public static final String FATAL_ERRORS_KEY = "FATAL_ERRORS";

    public static final boolean FATAL_ERRORS_DEFAULT_BOOLEAN = true;

    public static final String INPUT_BATCH_SIZE_KEY = "INPUT_BATCH_SIZE";

    public static final String INPUT_BATCH_SIZE_DEFAULT = "1";

    public static final String INPUT_CACHABLE_KEY = "INPUT_QUERY_CACHABLE";

    public static final String INPUT_CACHABLE_DEFAULT = "" + false;

    public static final String INPUT_COLLECTION_URI_KEY = "INPUT_COLLECTION_URI";

    public static final String INPUT_CONNECTION_STRING_KEY = "INPUT_CONNECTION_STRING";

    public static final String INPUT_DIRECTORY_URI_KEY = "INPUT_DIRECTORY_URI";

    public static final String INPUT_DOCUMENT_URIS_KEY = "INPUT_DOCUMENT_URIS";

    public static final String INPUT_MODULE_URI_KEY = "INPUT_MODULE_URI";

    public static final String INPUT_PACKAGE_KEY = "INPUT_PACKAGE";

    public static final String INPUT_PATH_KEY = "INPUT_PATH";

    public static final String INPUT_QUERY_KEY = "INPUT_QUERY";

    public static final String INPUT_QUERY_BUFFER_BYTES_KEY = "INPUT_QUERY_BUFFER_BYTES";

    public static final String INPUT_QUERY_BUFFER_BYTES_DEFAULT = "0";

    public static final String INPUT_RESULT_BUFFER_SIZE_KEY = "INPUT_RESULT_BUFFER_SIZE";

    public static final String INPUT_RESULT_BUFFER_SIZE_DEFAULT = "0";

    public static final String INPUT_START_POSITION_KEY = "INPUT_START_POSITION";

    public static final String INPUT_TIMESTAMP_AUTO = "#AUTO";

    public static final String INPUT_TIMESTAMP_KEY = "INPUT_TIMESTAMP";

    public static final String OUTPUT_COLLECTIONS_KEY = "OUTPUT_COLLECTIONS";

    public static final String OUTPUT_CONNECTION_STRING_KEY = "OUTPUT_CONNECTION_STRING";

    public static final String OUTPUT_FILTER_FORMATS_KEY = "OUTPUT_FILTER_FORMATS";

    public static final String OUTPUT_FORESTS_KEY = "OUTPUT_FORESTS";

    public static final String OUTPUT_PACKAGE_KEY = "OUTPUT_PACKAGE";

    public static final String OUTPUT_PATH_KEY = "OUTPUT_PATH";

    public static final String QUEUE_SIZE_KEY = "QUEUE_SIZE";

    public static final String READ_PERMISSION_ROLES_KEY = "READ_PERMISSION_ROLES";

    public static final String REPAIR_INPUT_XML_KEY = "REPAIR_INPUT_XML";

    public static final String REPAIR_INPUT_XML_DEFAULT = "false";

    public static final String SKIP_EXISTING_KEY = "SKIP_EXISTING";

    public static final String THREADS_KEY = "THREADS";

    public static final String THREADS_DEFAULT = "1";

    public static final String URI_PREFIX_KEY = "URI_PREFIX";

    /* internal constants */

    protected static final String CSV_SCSV_SSV_REGEX = "[,;\\s]+";

    protected static final String XCC_PREFIX = "xcc://";

    protected static final String XCC_PREFIX_OLD = "xdbc://";

    /* fields */

    protected Collection<ContentPermission> readRoles;

    protected String[] placeKeys = null;

    protected Connection outputConnection = null;

    protected String outputPath;

    protected boolean firstConfiguration = true;

    protected Connection inputConnection;

    protected String inputPath;

    protected String inputPackagePath;

    protected String outputPackagePath;

    protected Long startPosition;

    protected String uriPrefix;

    protected String[] outputCollections;

    protected String[] outputFormatFilters;

    protected BigInteger timestamp;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.ps.PropertyClientInterface#setProperties(java.util.Properties
     * )
     */
    public synchronized void setProperties(Properties _properties)
            throws XccException, IOException, URISyntaxException,
            SyncException {
        properties = _properties;

        // we need a logger as soon as possible: keep this first
        if (null == logger) {
            throw new NullPointerException("null logger");
        }
        logger.setProperties(properties);

        if (firstConfiguration) {
            logger.info("first-time setup");
            configure();
        }

        uriPrefix = properties.getProperty(URI_PREFIX_KEY);

        String readRolesString = properties
                .getProperty(READ_PERMISSION_ROLES_KEY);
        if (readRolesString != null) {
            logger.fine("read roles are: " + readRolesString);
            String[] roleNames = readRolesString
                    .split(CSV_SCSV_SSV_REGEX);
            if (roleNames.length > 0) {
                readRoles = new Vector<ContentPermission>();
                for (int i = 0; i < roleNames.length; i++) {
                    logger.fine("adding read role: " + roleNames[i]);
                    readRoles.add(new ContentPermission(
                            ContentPermission.READ, roleNames[i]));
                }
            }
        }

        String placeKeysString = properties
                .getProperty(OUTPUT_FORESTS_KEY);
        if (placeKeysString != null) {
            placeKeys = placeKeysString.split(CSV_SCSV_SSV_REGEX);
        }

        String outputFormatFilterString = properties
                .getProperty(OUTPUT_FILTER_FORMATS_KEY);
        if (null != outputFormatFilterString) {
            outputFormatFilterString = outputFormatFilterString.trim();
            if (null != outputFormatFilterString
                    && outputFormatFilterString.length() > 1) {
                outputFormatFilters = outputFormatFilterString
                        .split(CSV_SCSV_SSV_REGEX);
                logger.finest(this + " outputFormatFilters = "
                        + Utilities.join(outputFormatFilters, ","));
            }
        }

        String outputCollectionsString = properties
                .getProperty(OUTPUT_COLLECTIONS_KEY);
        if (null != outputCollectionsString) {
            outputCollectionsString = outputCollectionsString.trim();
            if (null != outputCollectionsString
                    && outputCollectionsString.length() > 1) {
                outputCollections = outputCollectionsString
                        .split(CSV_SCSV_SSV_REGEX);
                logger.finest(this + " outputCollections = "
                        + Utilities.join(outputCollections, ","));
            }
        }

    }

    /**
     * @throws XccException
     * @throws IOException
     * @throws URISyntaxException
     * @throws SyncException
     * 
     */
    private void configure() throws XccException, IOException,
            URISyntaxException, SyncException {
        // cold configuration
        if (!firstConfiguration) {
            return;
        }

        firstConfiguration = false;

        logger = SimpleLogger.getSimpleLogger();
        logger.configureLogger(properties);

        try {
            setDefaults();
        } catch (Exception e) {
            // crude, but this is a configuration-time error
            throw new FatalException(e);
        }
        validateProperties();

        if (properties == null || !properties.keys().hasMoreElements()) {
            logger.warning("null or empty properties");
        }

        // figure out the input source
        configureInput();

        // decide on the output
        configureOutput();

        configureTimestamp(properties.getProperty(INPUT_TIMESTAMP_KEY));

        // miscellaneous
        String startPositionString = properties
                .getProperty(INPUT_START_POSITION_KEY);
        if (startPositionString != null) {
            startPosition = new Long(startPositionString);
            if (startPosition.longValue() < 2) {
                startPosition = null;
            }
        }

    }

    private void configureInput() throws IOException, URISyntaxException,
            XccException, SyncException {
        inputPackagePath = properties.getProperty(INPUT_PACKAGE_KEY);

        if (null != inputPackagePath) {
            logger.info("input from package: " + inputPackagePath);
            return;
        }

        inputPath = properties.getProperty(INPUT_PATH_KEY);
        if (inputPath != null) {
            logger.info("input from path: " + inputPath);
        } else {
            String inputConnectionString = properties
                    .getProperty(INPUT_CONNECTION_STRING_KEY);
            if (inputConnectionString == null) {
                throw new IOException("missing required property: "
                        + INPUT_CONNECTION_STRING_KEY);
            }
            if (!(inputConnectionString.startsWith(XCC_PREFIX) || inputConnectionString
                    .startsWith(XCC_PREFIX_OLD))) {
                throw new SyncException("unsupported connection string: "
                        + inputConnectionString);
            }
            logger
                    .info("input from connection: "
                            + inputConnectionString);
            // split for load balancing
            String[] inputStrings = inputConnectionString
                    .split(CSV_SCSV_SSV_REGEX);
            URI[] inputUri = new URI[inputStrings.length];
            for (int i = 0; i < inputUri.length; i++) {
                inputUri[i] = new URI(inputStrings[i]);
            }
            inputConnection = new Connection(inputUri);

        }
    }

    private void configureOutput() throws IOException,
            URISyntaxException, XccException {

        outputPackagePath = properties.getProperty(OUTPUT_PACKAGE_KEY);

        if (null == outputPackagePath) {
            outputPath = properties.getProperty(OUTPUT_PATH_KEY);
            if (outputPath != null) {
                logger.info("output to path: " + outputPath);
                // not a zip file
                File outputFile = new File(outputPath);
                ensureFileExists(outputFile);
                outputPath = outputFile.getCanonicalPath();
            } else {
                String outputConnectionString = properties
                        .getProperty(OUTPUT_CONNECTION_STRING_KEY);
                if (outputConnectionString == null) {
                    throw new IOException("missing required property: "
                            + OUTPUT_CONNECTION_STRING_KEY);
                }
                if (!(outputConnectionString.startsWith(XCC_PREFIX) || outputConnectionString
                        .startsWith(XCC_PREFIX_OLD))) {
                    throw new UnimplementedFeatureException(
                            "unsupported connection string: "
                                    + outputConnectionString);
                }
                logger.info("output to connection: "
                        + outputConnectionString);
                URI outputUri = new URI(outputConnectionString);
                outputConnection = new Connection(outputUri);
            }
        } else {
            logger.info("output to package: " + outputPackagePath);
        }
    }

    /**
     * @return
     */
    public boolean isCopyCollections() {
        return Utilities.stringToBoolean(properties
                .getProperty(COPY_COLLECTIONS_KEY));
    }

    /**
     * @return
     */
    public boolean isCopyPermissions() {
        return Utilities.stringToBoolean(properties
                .getProperty(COPY_PERMISSIONS_KEY));
    }

    /**
     * @return
     */
    public boolean isCopyProperties() {
        return Utilities.stringToBoolean(properties
                .getProperty(COPY_PROPERTIES_KEY));
    }

    /**
     * @return
     */
    public boolean isCopyQuality() {
        return Utilities.stringToBoolean(properties
                .getProperty(COPY_QUALITY_KEY));
    }

    /**
     * @return
     */
    public boolean isFatalErrors() {
        return Utilities.stringToBoolean(properties
                .getProperty(FATAL_ERRORS_KEY));
    }

    /**
     * @return
     */
    public boolean isRepairInputXml() {
        return Utilities.stringToBoolean(properties
                .getProperty(REPAIR_INPUT_XML_KEY));
    }

    public String[] getPlaceKeys() {
        return placeKeys;
    }

    public Collection<ContentPermission> getReadRoles() {
        return readRoles;
    }

    /**
     * @return
     */
    public boolean isDeleteOutputCollection() {
        return Utilities.stringToBoolean(properties.getProperty(
                DELETE_COLLECTION_KEY, "false"));
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
    public com.marklogic.ps.Session newOutputSession() {
        if (null == outputConnection) {
            return null;
        }
        synchronized (outputConnection) {
            return (Session) outputConnection.newSession();
        }
    }

    /**
     * @return
     */
    public String getOutputPackagePath() {
        return outputPackagePath;
    }

    /**
     * @return
     */
    public String getOutputPath() {
        return outputPath;
    }

    /**
     * @param outputFile
     * @throws IOException
     */
    private static void ensureFileExists(File outputFile)
            throws IOException {
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
     * @return
     */
    public int getThreadCount() {
        return Integer.parseInt(properties.getProperty(THREADS_KEY));
    }

    /**
     * @return
     */
    public Session newInputSession() {
        logger.fine(null == inputConnection ? null : inputConnection
                .toString());
        if (null == inputConnection) {
            return null;
        }
        synchronized (inputConnection) {
            return (com.marklogic.ps.Session) inputConnection
                    .newSession();
        }
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
    public String getInputPackagePath() {
        return inputPackagePath;
    }

    /**
     * @return
     */
    public Long getStartPosition() {
        return startPosition;
    }

    /**
     * @param _key
     * @param _pattern
     * @return
     */
    private String[] getDelimitedPropertyValues(String _key,
            String _pattern) {
        String property = properties.getProperty(_key);
        logger.fine(_key + "=" + property + " using " + _pattern);
        if (null == property) {
            return null;
        }
        return property.split(_pattern);
    }

    /**
     * @param _key
     * @return
     */
    private String[] getDelimitedPropertyValues(String _key) {
        return getDelimitedPropertyValues(_key, "\\s+");
    }

    /**
     * @return
     */
    public String[] getInputCollectionUris() {
        return getDelimitedPropertyValues(INPUT_COLLECTION_URI_KEY);
    }

    /**
     * @return
     */
    public String[] getInputDirectoryUris() {
        return getDelimitedPropertyValues(INPUT_DIRECTORY_URI_KEY);
    }

    /**
     * @return
     */
    public String[] getInputDocumentUris() {
        return getDelimitedPropertyValues(INPUT_DOCUMENT_URIS_KEY);
    }

    /**
     * @return
     */
    public String[] getInputQuery() {
        // handle multiple queries, delimited by repeated semicolons
        return getDelimitedPropertyValues(INPUT_QUERY_KEY, ";;+");
    }

    /**
     * @return
     */
    public boolean isSkipExisting() {
        return Utilities.stringToBoolean(properties
                .getProperty(SKIP_EXISTING_KEY));

    }

    /**
     * @return
     */
    public static String getPackageFileExtension() {
        return OutputPackage.EXTENSION;
    }

    /**
     * @return
     */
    public String getUriPrefix() {
        return uriPrefix;
    }

    public String[] getOutputCollections() {
        logger.finest(this + " outputCollections = "
                + Utilities.join(outputCollections, ","));
        return outputCollections;
    }

    /**
     * @return
     */
    public boolean hasOutputCollections() {
        return null != outputCollections && outputCollections.length > 0;
    }

    /**
     * @return
     */
    public boolean isAllowEmptyMetadata() {
        return Utilities.stringToBoolean(properties
                .getProperty(ALLOW_EMPTY_METADATA_KEY));
    }

    /**
     * @return
     */
    public BigInteger getTimestamp() {
        return timestamp;
    }

    /**
     * @return
     */
    public boolean isInputQueryCachable() {
        return Boolean.parseBoolean(properties
                .getProperty(INPUT_CACHABLE_KEY));
    }

    /**
     * @return
     */
    public int inputQueryBufferSize() {
        return Integer.parseInt(properties
                .getProperty(INPUT_QUERY_BUFFER_BYTES_KEY));
    }

    /**
     * @return
     */
    public int getQueueSize() {
        return Integer.parseInt(properties.getProperty(QUEUE_SIZE_KEY, ""
                + (100 * 1000)))
                / getInputBatchSize();
    }

    /**
     * @return
     */
    public String[] getOutputFormatFilters() {
        return outputFormatFilters;
    }

    /**
     * @return
     */
    public String getInputModule() {
        return properties.getProperty(INPUT_MODULE_URI_KEY);
    }

    /**
     * @param _timestampString
     * @throws RequestException
     * 
     */
    private void configureTimestamp(String _timestampString)
            throws RequestException {
        if (null != _timestampString) {
            Session sess = newInputSession();
            if (null == sess) {
                logger.warning("ignoring "
                        + Configuration.INPUT_TIMESTAMP_KEY + "="
                        + _timestampString + " because "
                        + Configuration.INPUT_CONNECTION_STRING_KEY
                        + " is not set.");
            } else if (_timestampString.startsWith("#")) {
                // handle special values
                if (Configuration.INPUT_TIMESTAMP_AUTO
                        .equals(_timestampString)) {
                    // fetch the current timestamp
                    timestamp = sess.getCurrentServerPointInTime();
                } else {
                    logger.warning("ignoring unknown timestamp "
                            + _timestampString);
                }
            } else {
                timestamp = new BigInteger(_timestampString);
            }

            if (null != timestamp) {
                logger.info("using timestamp " + timestamp);
            }
        }
    }

    /**
     * @return
     */
    public int inputResultBufferSize() {
        return Integer.parseInt(properties
                .getProperty(INPUT_RESULT_BUFFER_SIZE_KEY));
    }

    /**
     * @return
     */
    public int getInputBatchSize() {
        return Integer.parseInt(properties
                .getProperty(INPUT_BATCH_SIZE_KEY));
    }

    /**
     * @return
     */
    public boolean isOutputConnection() {
        return null != outputConnection;
    }

}
