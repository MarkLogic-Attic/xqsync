/**
 * Copyright (c)2004-2008 Mark Logic Corporation
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import com.marklogic.ps.Session;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.ps.timing.TimedEvent;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.exceptions.XQueryException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class CallableSync implements Callable<TimedEvent> {

    private Session outputSession;

    private OutputPackage outputPackage;

    private SimpleLogger logger;

    private Collection<ContentPermission> readRoles;

    private String[] placeKeys;

    private String inputUri;

    private Session inputSession;

    private boolean copyPermissions;

    private boolean copyProperties;

    private InputPackage inputPackage;

    private File inputFile;

    private boolean skipExisting;

    private String outputPrefix;

    private String[] outputCollections;

    private boolean repairInputXml;

    private boolean allowEmptyMetadata;

    private BigInteger timestamp;

    private String[] outputFormatFilters = null;

    /**
     * @param _package
     * @param _path
     */
    public CallableSync(InputPackage _package, String _path) {
        inputPackage = _package;
        inputUri = _path;
    }

    /**
     * @param _session
     * @param _uri
     */
    public CallableSync(Session _session, String _uri) {
        inputSession = _session;
        inputUri = _uri;
    }

    /**
     * @param _file
     * @throws IOException
     */
    public CallableSync(File _file) {
        inputFile = _file;
        // note: don't set inputUri, since we can always get it from the file
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public TimedEvent call() throws Exception {
        // note: if there's an input file, then inputUri may be null
        if (null == inputUri) {
            if (null == inputFile) {
                throw new UnimplementedFeatureException(
                        "missing required field: inputUri or inputFile");
            }
            inputUri = inputFile.getCanonicalPath();
        }

        // try to avoid starvation
        Thread.yield();
        logger.fine("starting sync of " + inputUri);
        TimedEvent te = new TimedEvent();

        XQSyncDocument document;

        if (inputSession != null) {
            document = new XQSyncDocument(inputSession, inputUri,
                    copyPermissions, copyProperties, repairInputXml,
                    timestamp);
        } else if (inputPackage != null) {
            document = new XQSyncDocument(inputPackage, inputUri,
                    copyPermissions, copyProperties, repairInputXml,
                    allowEmptyMetadata);
        } else if (inputFile != null) {
            document = new XQSyncDocument(inputFile, copyPermissions,
                    copyProperties, repairInputXml, allowEmptyMetadata);
        } else {
            throw new UnimplementedFeatureException("no input found");
        }

        // try to avoid starvation
        Thread.yield();

        // write document to output session, package, or directory
        // marshal output arguments
        document.setOutputUriPrefix(outputPrefix);

        // handle output collections
        document.addOutputCollections(outputCollections);

        try {
            if (matchesFilters(document)) {
                // do not write
                // TODO correct accounting?
            } else if (outputSession != null) {
                document.write(outputSession, readRoles, placeKeys,
                        skipExisting);
            } else if (outputPackage != null) {
                document.write(outputPackage, readRoles);
                outputPackage.flush();
            } else {
                // default: filesystem
                document.write();
            }

            te.setDescription(document.getOutputUri());
            te.stop(document.getContentBytesLength());
            return te;
        } catch (XQueryException e) {
            if (null != inputPackage) {
                logger.warning("error in input package "
                        + inputPackage.getPath());
            }
            throw e;
        } finally {
            if (outputSession != null) {
                outputSession.close();
            }

            // try to avoid starvation
            Thread.yield();
        }
    }

    /**
     * @param document
     * @return
     */
    private boolean matchesFilters(XQSyncDocument document) {
        // return true if any filter matches

        // check format
        if (outputFormatFilters != null
                && Arrays.binarySearch(outputFormatFilters, document
                        .getMetadata().getFormatName()) > -1) {
            logger.finer(Configuration.OUTPUT_FILTER_FORMATS_KEY
                    + " matched " + document.getOutputUri());
            return true;
        }

        return false;
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
    public void addOutputCollections(String[] _outputCollections) {
        if (null == _outputCollections) {
            logger.finest(null);
            return;
        }
        logger.finest(Utilities.join(_outputCollections, ","));
        if (null == outputCollections) {
            outputCollections = _outputCollections;
        } else {
            List<String> tmp = Arrays.asList(outputCollections);
            tmp.addAll(Arrays.asList(_outputCollections));
            outputCollections = tmp.toArray(new String[0]);
        }
        logger.finest(Utilities.join(outputCollections, ","));
    }

    /**
     * @param _timestamp
     */
    public void setTimestamp(BigInteger _timestamp) {
        timestamp = _timestamp;
    }

    /**
     * @param _copyPermissions
     */
    public void setCopyPermissions(boolean _copyPermissions) {
        copyPermissions = _copyPermissions;
    }

    /**
     * @param _copyProperties
     */
    public void setCopyProperties(boolean _copyProperties) {
        copyProperties = _copyProperties;
    }

    /**
     * @param repairInputXml2
     */
    public void setRepairInputXml(boolean _repairInputXml) {
        repairInputXml = _repairInputXml;
    }

    /**
     * @param _allowEmptyMetadata
     */
    public void setAllowEmptyMetadata(boolean _allowEmptyMetadata) {
        allowEmptyMetadata = _allowEmptyMetadata;
    }

    /**
     * @param _outputFormatFilters
     */
    public void setOutputFormatFilters(String[] _outputFormatFilters) {
        outputFormatFilters = _outputFormatFilters;
    }

}
