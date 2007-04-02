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
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;

import com.marklogic.ps.Session;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.xcc.ContentPermission;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class TaskFactory {

    private boolean copyPermissions;

    private boolean copyProperties;

    private boolean skipExisting;

    private InputPackage inputPackage;

    private String[] placeKeys;

    private Collection<ContentPermission> readRoles;

    private OutputPackage outputPackage;

    private SimpleLogger logger;

    private Configuration configuration;

    private String prefix;

    private String[] outputCollections;

    /**
     * @param _config
     */
    public TaskFactory(Configuration _config) {
        configuration = _config;
        logger = _config.getLogger();
        copyPermissions = _config.isCopyPermissions();
        copyProperties = _config.isCopyProperties();
        skipExisting = _config.isSkipExisting();

        readRoles = _config.getReadRoles();
        placeKeys = _config.getPlaceKeys();

        // TODO filesystem output broken?
        //outputPath = _config.getOutputPath();
        String outputPackagePath = _config.getOutputPackagePath();
        if (outputPackagePath != null) {
            outputPackage = new OutputPackage(new File(outputPackagePath));
        }
        
        prefix = _config.getUriPrefix();
        
        if (_config.hasOutputCollections()) {
            outputCollections = _config.getOutputCollections();
        }
    }

    /**
     * @param cs
     */
    private void configure(CallableSync cs) {
        cs.setLogger(logger);

        Session outputSession = configuration.newOutputSession();
        cs.setOutputPrefix(prefix);
        if (null != outputSession) {
            cs.setOutputSession(outputSession);
            cs.setSkipExisting(skipExisting);
        } else if (null != outputPackage) {
            cs.setOutputPackage(outputPackage);
        }

        if (null != readRoles) {
            cs.setReadRoles(readRoles);
        }

        if (null != placeKeys) {
            cs.setPlaceKeys(placeKeys);
        }
        
        if (null != outputCollections) {
            cs.setOutputCollections(outputCollections);
        }
    }

    /**
     * @param file
     * @return
     */
    public Callable<String> newCallableSync(File file) {
        CallableSync cs = new CallableSync(file, copyPermissions,
                copyProperties);
        configure(cs);
        return cs;
    }

    /**
     * @param uri
     * @return
     */
    public Callable<String> newCallableSync(String uri) {
        CallableSync cs;
        if (inputPackage != null) {
            cs = new CallableSync(inputPackage, uri, copyPermissions,
                    copyProperties);
        } else {
            Session session = configuration.newInputSession();
            cs = new CallableSync(session, uri, copyPermissions,
                    copyProperties);
        }
        configure(cs);
        return cs;
    }

    /**
     * @param inputPackage
     */
    public void setInputPackage(InputPackage inputPackage) {
        this.inputPackage = inputPackage;
        InputPackage.setLogger(logger);
    }

    /**
     * 
     */
    public void close() {
        if (outputPackage != null) {
            try {
                outputPackage.close();
            } catch (IOException e) {
                logger.logException("cleanup", e);
            }
        }
    }

}
