/* -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c)2004-2012 MarkLogic Corporation
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
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Properties;
import java.util.Vector;
import java.util.Map;
import java.util.ArrayList;

import com.marklogic.ps.Connection;
import com.marklogic.ps.Session;
import com.marklogic.ps.Utilities;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class Configuration extends AbstractConfiguration {

    public static final String ALLOW_EMPTY_METADATA_KEY = "ALLOW_EMPTY_METADATA";

    public static final String ALLOW_EMPTY_METADATA_DEFAULT = "false";

    public static final String CONFIGURATION_CLASSNAME_KEY = "CONFIGURATION_CLASSNAME";

    public static final String CONFIGURATION_CLASSNAME_DEFAULT = Configuration.class
            .getCanonicalName();

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

    public static final String OUTPUT_BATCH_SIZE_KEY = "OUTPUT_BATCH_SIZE";

    public static final String OUTPUT_BATCH_SIZE_DEFAULT = "1";

    public static final String USE_MULTI_STMT_TXN_KEY = "USE_MULTI_STMT_TXN";

    public static final String USE_MULTI_STMT_TXN_DEFAULT = "false";

    public static final String ENCODE_OUTPUT_URI_KEY = "ENCODE_OUTPUT_URI";

    public static final String ENCODE_OUTPUT_URI_DEFAULT = "false";

    public static final String USE_RANDOM_OUTPUT_URI_KEY = "USE_RANDOM_OUTPUT_URI";

    public static final String USE_RANDOM_OUTPUT_URI_DEFAULT = "false";

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

    public static final String INPUT_INDENTED_KEY = "INPUT_INDENTED";

    public static final String MAX_RETRIES_DEFAULT = "5";

    public static final String MAX_RETRIES_KEY = "MAX_RETRIES";

    public static final String OUTPUT_COLLECTIONS_KEY = "OUTPUT_COLLECTIONS";

    public static final String OUTPUT_CONNECTION_STRING_KEY = "OUTPUT_CONNECTION_STRING";

    public static final String OUTPUT_FILTER_FORMATS_KEY = "OUTPUT_FILTER_FORMATS";

    public static final String OUTPUT_FORESTS_KEY = "OUTPUT_FORESTS";

    public static final String OUTPUT_PACKAGE_KEY = "OUTPUT_PACKAGE";

    public static final String OUTPUT_PATH_KEY = "OUTPUT_PATH";

    public static final String QUEUE_SIZE_KEY = "QUEUE_SIZE";

    public static final String ROLES_READ_KEY = "ROLES_READ";

    public static final String ROLES_UPDATE_KEY = "ROLES_UPDATE";

    public static final String ROLES_INSERT_KEY = "ROLES_INSERT";

    public static final String ROLES_EXECUTE_KEY = "ROLES_EXECUTE";

    public static final String REPAIR_INPUT_XML_KEY = "REPAIR_INPUT_XML";

    public static final String REPAIR_INPUT_XML_DEFAULT = "false";

    public static final String REPAIR_MULTIPLE_DOCUMENTS_PER_URI_DEFAULT = "false";

    public static final String REPAIR_MULTIPLE_DOCUMENTS_PER_URI_KEY = "REPAIR_MULTIPLE_DOCUMENTS_PER_URI";

    public static final String SKIP_EXISTING_KEY = "SKIP_EXISTING";

    public static final String THREADS_KEY = "THREADS";

    public static final String THREADS_DEFAULT = "1";

    public static final String THROTTLE_EVENTS_KEY = "THROTTLE_EVENTS_PER_SECOND";

    public static final String THROTTLE_EVENTS_DEFAULT = "0";

    public static final String THROTTLE_BYTES_KEY = "THROTTLE_BYTES_PER_SECOND";

    public static final String THROTTLE_BYTES_DEFAULT = "0";

    public static final String URI_PREFIX_KEY = "URI_PREFIX";

    public static final String URI_SUFFIX_KEY = "URI_SUFFIX";

    public static final String URI_PREFIX_STRIP_KEY = "URI_PREFIX_STRIP";

    public static final String URI_SUFFIX_STRIP_KEY = "URI_SUFFIX_STRIP";

    public static final String USE_IN_MEMORY_URI_QUEUE_KEY = "USE_IN_MEMORY_URI_QUEUE";

    public static final String USE_IN_MEMORY_URI_QUEUE_DEFAULT = "false";

    public static final String TMP_DIR_KEY = "TMP_DIR";

    public static final String TMP_DIR_DEFAULT = null;

    public static final String URI_QUEUE_FILE_KEY = "URI_QUEUE_FILE";

    public static final String URI_QUEUE_FILE_DEFAULT = null;

    public static final String KEEP_URI_QUEUE_FILE_KEY = "KEEP_URI_QUEUE_FILE";

    public static final String KEEP_URI_QUEUE_FILE_DEFAULT = "false";

    public static final String PRINT_CURRENT_RATE_KEY = "PRINT_CURRENT_RATE";

    public static final String PRINT_CURRENT_RATE_DEFAULT = "false";

    public static final String USE_IN_FOREST_EVAL_KEY = "USE_IN_FOREST_EVAL";

    public static final String USE_IN_FOREST_EVAL_DEFAULT = "false";

    public static final String CHECKSUM_MODULE_KEY = "CHECKSUM_MODULE";

    /* internal constants */

    protected static final String CSV_SCSV_SSV_REGEX = "[,;\\s]+";

    protected static final String XCC_PREFIX = "xcc://";

    protected static final String XCCS_PREFIX = "xccs://";

    protected static final String XCC_PREFIX_OLD = "xdbc://";

    /* fields */

    protected Collection<ContentPermission> permissionRoles = new ArrayList<ContentPermission>();

    protected String[] placeKeys = null;

    protected Connection[] outputConnection = null;

    protected String outputPath;

    protected boolean firstConfiguration = true;

    protected Connection inputConnection;

    protected String inputPath;

    protected String inputPackagePath;

    protected String outputPackagePath;

    protected Long startPosition;

    protected double throttledEventsPerSecond;

    protected int throttledBytesPerSecond;

    protected String uriPrefix;

    protected String[] outputCollections;

    protected String[] outputFormatFilters;

    protected BigInteger timestamp;

    private int outputConnectionCount = 0;

    protected Map<String, BigInteger> forestMap = null;

    /*
     * (non-Javadoc)
     *
     * @see
     * com.marklogic.ps.PropertyClientInterface#setProperties(java.util.Properties
     * )
     */
    public synchronized void setProperties(Properties _properties)
            throws Exception {
        properties = _properties;

        // we need a logger as soon as possible in this method
        if (null == logger) {
            throw new NullPointerException("null logger");
        }
        logger.setProperties(properties);
    }

    /**
     * @throws XccException
     * @throws IOException
     * @throws URISyntaxException
     * @throws SyncException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     *
     */
    public void configure() throws Exception {
        // cold configuration
        if (!firstConfiguration) {
            return;
        }
        firstConfiguration = false;

        // we need a logger as soon as possible in this method
        if (null == logger) {
            throw new NullPointerException("null logger");
        }

        if (properties == null || !properties.keys().hasMoreElements()) {
            logger.warning("null or empty properties");
        }

        logger.info("first-time setup");
        logger.configureLogger(properties);

        try {
            setDefaults();
        } catch (Exception e) {
            // crude, but this is a configuration-time error
            throw new FatalException(e);
        }
        validateProperties();

        // figure out the input source
        configureInput();

        // decide on the output
        configureOutput();

        configureTimestamp(properties.getProperty(INPUT_TIMESTAMP_KEY));

        configureThrottling();

        // miscellaneous
        String startPositionString = properties
                .getProperty(INPUT_START_POSITION_KEY);
        if (startPositionString != null) {
            startPosition = new Long(startPositionString);
            if (startPosition.longValue() < 2) {
                startPosition = null;
            }
        }

        uriPrefix = properties.getProperty(URI_PREFIX_KEY);

        getPermissionRole(ROLES_READ_KEY, ContentPermission.READ);
        getPermissionRole(ROLES_UPDATE_KEY, ContentPermission.UPDATE);
        getPermissionRole(ROLES_INSERT_KEY, ContentPermission.INSERT);
        getPermissionRole(ROLES_EXECUTE_KEY, ContentPermission.EXECUTE);

        String placeKeysString = properties
                .getProperty(OUTPUT_FORESTS_KEY);
        if (placeKeysString != null) {
            placeKeys = placeKeysString.split(CSV_SCSV_SSV_REGEX);
            logger.info("placeKeys = " + placeKeysString);
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

    private void configureInput() throws IOException, URISyntaxException,
            XccException, SyncException, KeyManagementException,
            NoSuchAlgorithmException {
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
            if (!(isValidConnectionString(inputConnectionString))) {
                throw new SyncException("unsupported connection string: "
                        + inputConnectionString);
            }
            // split for load balancing
            String[] inputStrings = inputConnectionString
                    .split(CSV_SCSV_SSV_REGEX);
            URI[] inputUri = new URI[inputStrings.length];
            logger.info("input from connection: ");
            for (int i = 0; i < inputUri.length; i++) {
                inputUri[i] = new URI(inputStrings[i]);

                String[] splitStr = inputStrings[i].split("@");
                logger.info("input connection string: " + splitStr[1]);
            }
            inputConnection = new Connection(inputUri);
        }
    }

    protected void configureOutput() throws Exception {
        outputPackagePath = properties.getProperty(OUTPUT_PACKAGE_KEY);

        if (null != outputPackagePath) {
            logger.info("output to package: " + outputPackagePath);
        } else {
            outputPath = properties.getProperty(OUTPUT_PATH_KEY);
            if (null != outputPath) {
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
                if (!(isValidConnectionString(outputConnectionString))) {
                    throw new UnimplementedFeatureException(
                            "unsupported connection string: "
                                    + outputConnectionString);
                }
                String[] outputConnectionStrings = outputConnectionString
                        .split(CSV_SCSV_SSV_REGEX);
                outputConnection = new Connection[outputConnectionStrings.length];
                logger.info("output to connection: ");
                for (int i = 0; i < outputConnection.length; i++) {
                    outputConnection[i] = new Connection(new URI(
                            outputConnectionStrings[i]));

                    String[] splitStr = outputConnectionStrings[i].split("@");
                    logger.info("output connection string: " + splitStr[1]);
                }
            }
        }
    }

    /**
    *
    */
    void configureThrottling() {
        throttledEventsPerSecond = Double.parseDouble(properties
                .getProperty(THROTTLE_EVENTS_KEY));

        throttledBytesPerSecond = Integer.parseInt(properties
                .getProperty(THROTTLE_BYTES_KEY));
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
     * @param _connectionString
     * @return
     */
    protected boolean isValidConnectionString(String _connectionString) {
        return _connectionString.startsWith(XCC_PREFIX)
                || _connectionString.startsWith(XCCS_PREFIX)
                || _connectionString.startsWith(XCC_PREFIX_OLD);
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
    public boolean isRepairMultipleDocumentsPerUri() {
        return Utilities.stringToBoolean(properties
                .getProperty(REPAIR_MULTIPLE_DOCUMENTS_PER_URI_KEY));
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

    public Collection<ContentPermission> getPermissionRoles() {
        return permissionRoles;
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
    public com.marklogic.ps.Session newOutputSession() {
        return newOutputSession(null);
    }

    /**
     * @return a Session that would perform inForestEval
     */
    public com.marklogic.ps.Session newOutputSession(String forestId) {
        if (null == outputConnection) {
            return null;
        }
        // support round-robin across multiple outputs
        synchronized (outputConnection) {
            int x = (outputConnectionCount++ % outputConnection.length);
            return (forestId == null ?
                    (Session) outputConnection[x].newSession() :
                    (Session) outputConnection[x].newSession(forestId));
        }
    }

    /**
     * @return a map of forest names to forest ids
     */
    public Map<String, BigInteger> getOutputForestMap() {
        if (forestMap == null) {
            Session sess = newOutputSession();
            if (sess != null) {
                try {
                    forestMap = sess.getForestMap();
                } catch (XccException e) {
                    logger.warning("can't get forest map");
                }
                sess.close();
            }
        }
        return forestMap;
    }

    /**
     * @return an array of forest names
     */
    public String[] getOutputForestNames() {
        Map<String, BigInteger> fmap = getOutputForestMap();
        return fmap.keySet().toArray(new String[0]);
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
     * @return
     */
    public String getConfigurationClassName() {
        // keep the default - used to construct the pre-defaults configuration
        return properties.getProperty(CONFIGURATION_CLASSNAME_KEY,
                CONFIGURATION_CLASSNAME_DEFAULT);
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
    public boolean getInputIndented() {
        return Utilities.stringToBoolean(properties.getProperty(INPUT_INDENTED_KEY), true);
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
    public int getMaxRetries() {
        return Integer.parseInt(properties
                .getProperty(MAX_RETRIES_KEY));
    }

    /**
     * @return
     */
    public int getOutputBatchSize() {
        return Integer.parseInt(properties
                .getProperty(OUTPUT_BATCH_SIZE_KEY));
    }

    /**
     * @return
     */
    public boolean useMultiStmtTxn() {
        return Boolean.parseBoolean(properties
                .getProperty(USE_MULTI_STMT_TXN_KEY));
    }

    /**
     * @return
     */
    public boolean encodeOutputUri() {
        return Boolean.parseBoolean(properties
                .getProperty(ENCODE_OUTPUT_URI_KEY));
    }

    /**
     * @return
     */
    public boolean useRandomOutputUri() {
        return Boolean.parseBoolean(properties.getProperty(USE_RANDOM_OUTPUT_URI_KEY));
    }

    /**
     * @return
     */
    public boolean isOutputConnection() {
        return null != outputConnection;
    }

    /**
     * @return
     */
    public String getUriPrefixStrip() {
        return properties.getProperty(URI_PREFIX_STRIP_KEY);
    }

    /**
     * @return
     */
    public String getUriSuffixStrip() {
        return properties.getProperty(URI_SUFFIX_STRIP_KEY);
    }

    /**
     * @return
     */
    public String getUriSuffix() {
        return properties.getProperty(URI_SUFFIX_KEY);
    }

    /**
     * @return
     */
    public boolean isThrottled() {
        return (throttledEventsPerSecond > 0 || throttledBytesPerSecond > 0);
    }

    /**
     * @return
     */
    public int getThrottledBytesPerSecond() {
        return throttledBytesPerSecond;
    }

    /**
     * @return
     */
    public double getThrottledEventsPerSecond() {
        return throttledEventsPerSecond;
    }

    /**
     * @return whether to use the in memory uri queue or not
     */
    public boolean useInMemoryUriQueue() {
        String p = properties.getProperty(USE_IN_MEMORY_URI_QUEUE_KEY,
                                          USE_IN_MEMORY_URI_QUEUE_DEFAULT);
        return Boolean.parseBoolean(p);
    }

    /**
     * @return whether to use a file for uri queue or not
     */
    public boolean useQueueFile() {
        return (!useInMemoryUriQueue() &&
                properties.getProperty(INPUT_CONNECTION_STRING_KEY) != null);
    }


    /**
     * @return the temporary directory location
     */
    public String getTmpDir() {
        return properties.getProperty(TMP_DIR_KEY,
                                      TMP_DIR_DEFAULT);
    }


    /**
     * @return the uri queue file location
     */
    public String getUriQueueFile() {
        return properties.getProperty(URI_QUEUE_FILE_KEY,
                                      URI_QUEUE_FILE_DEFAULT);
    }

    /**
     * @return whether the queue file should be kept
     */
    public boolean keepUriQueueFile() {
        String p = properties.getProperty(KEEP_URI_QUEUE_FILE_KEY,
                                          KEEP_URI_QUEUE_FILE_DEFAULT);
        return Boolean.parseBoolean(p);
    }

    /**
     * @return boolean, true if we should print out the current rate
     */
    public boolean doPrintCurrRate() {
        String p = properties.getProperty(PRINT_CURRENT_RATE_KEY,
                                          PRINT_CURRENT_RATE_DEFAULT);
        return Boolean.parseBoolean(p);
    }

    /**
     * @return boolean, true if we should use in-forest eval
     */
    public boolean useInForestEval() {
        String p = properties.getProperty(USE_IN_FOREST_EVAL_KEY,
                                          USE_IN_FOREST_EVAL_DEFAULT);
        return Boolean.parseBoolean(p);
    }

    /**
     * @return whether hash module should be used or not
     */
    public boolean useChecksumModule() {
        String m = getChecksumModule();
        return (m != null && !m.isEmpty());
    }

    /**
     * @return
     */
    public String getChecksumModule() {
        return properties.getProperty(CHECKSUM_MODULE_KEY);
    }

    /**
     *
     */
    public void close() {
        // nothing to do
        logger.fine("closed");
    }

    /**
     * @return
     * @throws SyncException
     *
     *             This method always returns a SessionWriter or FilePathWriter.
     *             However, overriding subclasses can return any object that
     *             implements ReaderInterface.
     */
    public WriterInterface newWriter() throws SyncException {
        if (isOutputConnection()) {
            return new SessionWriter(this);
        }
        return new FilePathWriter(this);
    }

    /**
     * @throws SyncException
     *
     *             This method always returns a SessionReader object. However,
     *             overriding subclasses can return any object that implements
     *             WriterInterface.
     */
    public ReaderInterface newReader() throws SyncException {
        return new SessionReader(this);
    }

    private void getPermissionRole(String propertyKey, ContentCapability capability) {
        String rolesString = properties.getProperty(propertyKey);
        if (rolesString != null) {
            String[] roleNames = rolesString.split(CSV_SCSV_SSV_REGEX);
            for (int i = 0; i < roleNames.length; i++)
                permissionRoles.add(new ContentPermission(capability, roleNames[i]));
        }
    }

}
