/**
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
public class CallableSync implements Callable<String> {

    private XQSyncDocument document;

    private Session outputSession;

    private OutputPackage outputPackage;

    private Collection<ContentPermission> readRoles;

    private String[] placeKeys;

    private SimpleLogger logger;

    private String inputUri;

    private Session inputSession;

    private boolean copyPermissions;

    private boolean copyProperties;

    private InputPackage inputPackage;

    private File inputFile;

    private boolean skipExisting;

    private String outputPrefix;

    private String[] outputCollections;

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
     * @throws IOException
     */
    public CallableSync(File _file, boolean _copyPermissions,
            boolean _copyProperties) {
        inputFile = _file;
        // note: don't set inputUri, since we can always get it from the file
        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public String call() throws Exception {
        // note: if there's an input file, then inputUri may be null
        if (null == inputUri) {
            if (null == inputFile) {
                throw new UnimplementedFeatureException(
                        "missing required field: inputUri or inputFile");
            }
            inputUri = inputFile.getCanonicalPath();
        }
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
        document.setOutputUriPrefix(outputPrefix);
        
        // handle output collections
        document.setOutputCollections(outputCollections);

        try {
            if (outputSession != null) {
                document.write(outputSession, readRoles, placeKeys,
                        skipExisting);
            } else if (outputPackage != null) {
                document.write(outputPackage, readRoles);
                outputPackage.flush();
            } else {
                // default: filesystem
                document.write();
            }
            return document.getOutputUri();
        } finally {
            if (outputSession != null) {
                outputSession.close();
            }
        }
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

    /**
     * @param prefix
     */
    public void setOutputPrefix(String prefix) {
        outputPrefix = prefix;
    }

    /**
     * @param _outputCollections
     */
    public void setOutputCollections(String[] _outputCollections) {
        outputCollections = _outputCollections;
    }

}
