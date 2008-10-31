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
import java.util.Collection;
import java.util.concurrent.Callable;

import com.marklogic.ps.Session;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.ps.timing.TimedEvent;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.exceptions.RequestException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class TaskFactory {

    private boolean copyPermissions;

    private boolean copyProperties;

    private boolean skipExisting;

    private String[] placeKeys;

    private Collection<ContentPermission> readRoles;

    private OutputPackage[] outputPackages;

    private SimpleLogger logger;

    private Configuration configuration;

    private String prefix;

    private String[] outputCollections;

    private boolean repairInputXml;

    private boolean allowEmptyMetadata;

    private BigInteger timestamp = null;

    private int threadCount = Configuration.THREADS_DEFAULT_INT;

    private String[] outputFormatFilters = null;

    private String inputModule;

    /**
     * @param _config
     * @throws RequestException
     * @throws IOException
     */
    public TaskFactory(Configuration _config) throws RequestException,
            IOException {
        configuration = _config;
        logger = _config.getLogger();
        copyPermissions = _config.isCopyPermissions();
        copyProperties = _config.isCopyProperties();

        inputModule = _config.getInputModule();
        if (null != inputModule) {
            logger.info("using " + Configuration.INPUT_MODULE_URI_KEY
                    + "=" + inputModule);
        }
        repairInputXml = _config.isRepairInputXml();
        allowEmptyMetadata = _config.isAllowEmptyMetadata();

        skipExisting = _config.isSkipExisting();

        readRoles = _config.getReadRoles();
        placeKeys = _config.getPlaceKeys();

        threadCount = _config.getThreadCount();

        outputFormatFilters = _config.getOutputFormatFilters();

        // TODO filesystem output broken?
        // outputPath = _config.getOutputPath();

        String outputPackagePath = _config.getOutputPackagePath();
        if (outputPackagePath != null) {
            configureOutputPackages(outputPackagePath);
        }

        prefix = _config.getUriPrefix();

        logger.finest(this + " outputCollections = "
                + Utilities.join(_config.getOutputCollections(), ","));
        outputCollections = _config.getOutputCollections();
        logger.finest(this + " outputCollections = "
                + Utilities.join(outputCollections, ","));

        configureTimestamp();
    }

    /**
     * @param outputPackagePath
     * @throws IOException
     */
    private void configureOutputPackages(String outputPackagePath)
            throws IOException {
        String canonicalPath = new File(outputPackagePath)
                .getCanonicalPath();
        String path;

        // create enough output packages to avoid most thread contention
        int numPackages = Math.max(Runtime.getRuntime()
                .availableProcessors(), Math.min(1, threadCount / 2));
        outputPackages = new OutputPackage[numPackages];

        for (int i = 0; i < outputPackages.length; i++) {
            path = OutputPackage.newPackagePath(canonicalPath, i, 3);
            outputPackages[i] = new OutputPackage(new File(path));
        }
    }

    /**
     * @throws RequestException
     * 
     */
    private void configureTimestamp() throws RequestException {
        String timestampString = configuration.getTimestamp();
        if (null != timestampString) {
            Session sess = configuration.newInputSession();
            if (null == sess) {
                logger.warning("ignoring "
                        + Configuration.INPUT_TIMESTAMP_KEY + "="
                        + timestampString + " because "
                        + Configuration.INPUT_CONNECTION_STRING_KEY
                        + " is not set.");
            } else if (timestampString.startsWith("#")) {
                // handle special values
                if (Configuration.INPUT_TIMESTAMP_AUTO
                        .equals(timestampString)) {
                    // fetch the current timestamp
                    timestamp = sess.getCurrentServerPointInTime();
                } else {
                    logger.warning("ignoring unknown timestamp "
                            + timestampString);
                }
            } else {
                timestamp = new BigInteger(timestampString);
            }
            if (null != timestamp) {
                logger.info("using timestamp " + timestamp);
            }
        }
    }

    /**
     * @param cs
     */
    private void configure(CallableSync cs) {
        cs.setLogger(logger);

        // TODO place a config ref in the callablesync object?
        cs.setCopyPermissions(copyPermissions);
        cs.setCopyProperties(copyProperties);
        cs.setRepairInputXml(repairInputXml);
        cs.setAllowEmptyMetadata(allowEmptyMetadata);
        cs.setOutputFormatFilters(outputFormatFilters);
        cs.setInputModule(inputModule);

        Session outputSession = configuration.newOutputSession();
        cs.setOutputPrefix(prefix);
        if (null != outputSession) {
            cs.setOutputSession(outputSession);
            cs.setSkipExisting(skipExisting);
        } else if (null != outputPackages) {
            // very simple pooling - outputPackage is thread-safe, anyway,
            // but this keeps most threads from contending for a package
            cs.setOutputPackage(outputPackages[Thread.currentThread()
                    .hashCode()
                    % outputPackages.length]);
        }

        if (null != readRoles) {
            cs.setReadRoles(readRoles);
        }

        if (null != placeKeys) {
            cs.setPlaceKeys(placeKeys);
        }

        if (null != timestamp) {
            cs.setTimestamp(timestamp);
        }

        logger.finest("outputCollections = "
                + Utilities.join(outputCollections, ","));
        cs.addOutputCollections(outputCollections);
    }

    /**
     * @param file
     * @return
     */
    public Callable<TimedEvent> newCallableSync(File file) {
        CallableSync cs = new CallableSync(file);
        configure(cs);
        return cs;
    }

    /**
     * @param uri
     * @return
     */
    public Callable<TimedEvent> newCallableSync(String uri) {
        Session session = configuration.newInputSession();
        CallableSync cs = new CallableSync(session, uri);
        configure(cs);
        return cs;
    }

    /**
     * @param inputPackage
     * @param uri
     * @return
     */
    public Callable<TimedEvent> newCallableSync(
            InputPackage inputPackage, String uri) {
        CallableSync cs = new CallableSync(inputPackage, uri);
        configure(cs);
        return cs;
    }

    /**
     * 
     */
    public void close() {
        if (null != outputPackages) {
            logger.info("closing " + outputPackages.length
                    + " output package(s)");
            for (int i = 0; i < outputPackages.length; i++) {
                try {
                    outputPackages[i].close();
                } catch (IOException e) {
                    logger.logException("cleanup", e);
                }
            }
        }
    }

}
