/*
 * Copyright (c)2004-2007 Mark Logic Corporation
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Properties;
import java.util.Vector;

import com.marklogic.ps.AbstractLoggableClass;
import com.marklogic.ps.Connection;
import com.marklogic.ps.Session;
import com.marklogic.ps.Utilities;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Configuration extends AbstractLoggableClass {

    /**
     * 
     */
    public static final String COPY_PROPERTIES_KEY = "COPY_PROPERTIES";

    /**
     * 
     */
    public static final String COPY_PERMISSIONS_KEY = "COPY_PERMISSIONS";

    /**
     * 
     */
    public static final String SKIP_EXISTING_KEY = "SKIP_EXISTING";

    /**
     * 
     */
    public static final String FATAL_ERRORS_KEY = "FATAL_ERRORS";

    /**
     * 
     */
    public final String DELETE_COLLECTION_KEY = "DELETE_COLLECTION";

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
    public static final String INPUT_CONNECTION_STRING_KEY = "INPUT_CONNECTION_STRING";

    /**
     * 
     */
    public static final String INPUT_PATH_KEY = "INPUT_PATH";

    public static final String URI_PREFIX_KEY = "URI_PREFIX";

    /**
     * 
     */
    public static final String INPUT_PACKAGE_KEY = "INPUT_PACKAGE";

    public static final String INPUT_DIRECTORY_URI = "INPUT_DIRECTORY_URI";

    public static final String INPUT_COLLECTION_URI_KEY = "INPUT_COLLECTION_URI";

    private Properties properties;

    private boolean copyPermissions = true;

    private boolean copyProperties = true;

    private boolean fatalErrors = true;

    private Collection<ContentPermission> readRoles;

    private String[] placeKeys = null;

    private Connection outputConnection;

    private String outputPath;

    private boolean firstConfiguration = true;

    private Connection inputConnection;

    private String inputPath;

    private String inputPackagePath;

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
    private static final String READ_PERMISSION_ROLES_KEY = "READ_PERMISSION_ROLES";

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

    private String outputPackagePath;

    private Long startPosition;

    private boolean skipExisting = false;

    private String uriPrefix = null;

    public Configuration() {
        super();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.PropertyClientInterface#setProperties(java.util.Properties)
     */
    public synchronized void setProperties(Properties _properties)
            throws XccException, IOException, URISyntaxException {
        properties = _properties;
        
        // we need a logger as soon as possible: keep this first
        // logger config is hot
        logger.setProperties(_properties);

        if (firstConfiguration) {
            logger.info("first-time setup");
            configure();
        }

        uriPrefix = properties.getProperty(URI_PREFIX_KEY);

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
        copyProperties = Utilities.stringToBoolean(properties
                .getProperty(COPY_PROPERTIES_KEY, "true"));

        // skipExisting is hot
        skipExisting = Utilities.stringToBoolean(properties.getProperty(
                SKIP_EXISTING_KEY, "false"));

        // placeKeys are hot
        String placeKeysString = properties
                .getProperty(OUTPUT_FORESTS_KEY);
        if (placeKeysString != null)
            placeKeys = placeKeysString.split(",");

    }

    /**
     * @throws XccException
     * @throws IOException
     * @throws URISyntaxException
     * 
     */
    private void configure() throws XccException, IOException,
            URISyntaxException {
        // cold configuration
        if (!firstConfiguration) {
            return;
        }

        firstConfiguration = false;

        if (properties == null || !properties.keys().hasMoreElements()) {
            throw new IOException("null or empty properties");
        }

        // figure out the input source
        configureInput();

        // decide on the output
        configureOutput();

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
            XccException {
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
                logger.fine("fixing connection string "
                        + inputConnectionString);
                inputConnectionString = XCC_PREFIX
                        + inputConnectionString;
            }
            logger
                    .info("input from connection: "
                            + inputConnectionString);
            URI inputUri = new URI(inputConnectionString);
            inputConnection = new Connection(inputUri);
        }
    }

    private void configureOutput() throws IOException,
            URISyntaxException, XccException {
        outputPackagePath = properties.getProperty(OUTPUT_PACKAGE_KEY);

        if (outputPackagePath == null) {
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
                    logger.fine("fixing connection string "
                            + outputConnectionString);
                    outputConnectionString = XCC_PREFIX
                            + outputConnectionString;
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

    public boolean isCopyPermissions() {
        return copyPermissions;
    }

    public boolean isCopyProperties() {
        return copyProperties;
    }

    public boolean isFatalErrors() {
        return fatalErrors;
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
        if (outputConnection == null) {
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
        return new Integer(properties.getProperty(THREADS_KEY,
                THREADS_DEFAULT)).intValue();
    }

    /**
     * @return
     */
    public Session newInputSession() {
        if (inputConnection == null) {
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
     * @return
     */
    public String[] getInputCollectionUris() {
        String property = properties
                .getProperty(INPUT_COLLECTION_URI_KEY);
        if (property == null) {
            return null;
        }
        return property.split("\\s+");
    }

    /**
     * @return
     */
    public String[] getInputDirectoryUris() {
        String property = properties.getProperty(INPUT_DIRECTORY_URI);
        if (property == null) {
            return null;
        }
        return property.split("\\s+");
    }

    /**
     * @return
     */
    public String[] getInputDocumentUris() {
        String property = properties.getProperty(INPUT_DOCUMENT_URIS_KEY);
        if (property == null) {
            return null;
        }
        return property.split("\\s+");
    }

    /**
     * @return
     */
    public String getInputQuery() {
        return properties.getProperty(INPUT_QUERY_KEY);
    }

    /**
     * @return
     */
    public boolean isSkipExisting() {
        return skipExisting;
    }

    /**
     * @return
     */
    public static String getPackageFileExtension() {
        return OutputPackage.EXTENSION;
    }

    // TODO uriSuffix impl
    
    /**
     * @return
     */
    public String getUriPrefix() {
        return uriPrefix;
    }

}
