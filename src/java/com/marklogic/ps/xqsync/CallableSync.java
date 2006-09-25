/**
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
import java.util.Collection;
import java.util.concurrent.Callable;

import com.marklogic.ps.Session;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class CallableSync implements Callable<Object> {

    private XQSyncDocument document;

    private Session outputSession;

    private OutputPackage outputPackage;

    private Collection<ContentPermission> readRoles;

    private String[] placeKeys;

    private String outputPath;

    private SimpleLogger logger;

    private String inputUri;

    private Session inputSession;

    private boolean copyPermissions;

    private boolean copyProperties;

    private InputPackage inputPackage;

    private File inputFile;

    private boolean skipExisting;

    /**
     * @param _path
     * @param _copyPermissions
     * @param _copyProperties
     */
    public CallableSync(InputPackage _package, String _path,
            boolean _copyPermissions, boolean _copyProperties) {
        inputPackage = _package;
        inputUri = _path;
        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;
    }

    /**
     * @param _session
     * @param _uri
     * @param _copyPermissions
     * @param _copyProperties
     */
    public CallableSync(Session _session, String _uri,
            boolean _copyPermissions, boolean _copyProperties) {
        inputSession = _session;
        inputUri = _uri;
        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;
    }

    /**
     * @param _file
     * @param _copyPermissions
     * @param _copyProperties
     */
    public CallableSync(File _file, boolean _copyPermissions,
            boolean _copyProperties) {
        inputFile = _file;
        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public String call() throws Exception {
        logger.fine("starting sync of " + inputUri);

        if (inputSession != null) {
            document = new XQSyncDocument(inputSession, inputUri,
                    copyPermissions, copyProperties);
        } else if (inputPackage != null) {
            document = new XQSyncDocument(inputPackage, inputUri,
                    copyPermissions, copyProperties);
        } else if (inputFile != null) {
            document = new XQSyncDocument(inputFile, copyPermissions,
                    copyProperties);
        } else {
            throw new UnimplementedFeatureException("no input found");
        }

        // write document to output session, package, or directory
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
        String outputUri = (path + inputUri).replaceAll("//+", "/");
        logger.finer("copying " + inputUri + " to " + outputUri);

        try {
            if (outputSession != null) {
                document.write(outputUri, outputSession, readRoles,
                        placeKeys, skipExisting);
            } else if (outputPackage != null) {
                document.write(outputUri, outputPackage, readRoles);
                outputPackage.flush();
            } else {
                // default: filesystem
                File outputFile = new File(outputUri);
                document.write(outputFile);
            }
            return outputUri;
        } finally {
            if (outputSession != null) {
                outputSession.close();
            }
        }
    }

    public void setOutputPath(String _path) {
        this.outputPath = _path;
    }

    public void setOutputPackage(OutputPackage outputPackage) {
        this.outputPackage = outputPackage;
    }

    public void setOutputSession(Session outputSession) {
        this.outputSession = outputSession;
    }

    public void setPlaceKeys(String[] placeKeys) {
        this.placeKeys = placeKeys;
    }

    public void setReadRoles(Collection<ContentPermission> readRoles) {
        this.readRoles = readRoles;
    }

    /**
     * @param logger
     */
    public void setLogger(SimpleLogger logger) {
        this.logger = logger;
    }

    public void setSkipExisting(boolean skipExisting) {
        this.skipExisting = skipExisting;
    }

}
