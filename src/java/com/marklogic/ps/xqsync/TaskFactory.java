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

import com.marklogic.ps.Connection;
import com.marklogic.ps.Session;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class TaskFactory {

    private boolean copyPermissions;

    private boolean copyProperties;

    private XQSyncPackage inputPackage;

    private Connection inputConnection;

    private String[] placeKeys;

    private Collection<ContentPermission> readRoles;

    private Session outputSession;

    private String outputPath;

    private XQSyncPackage outputPackage;

    private SimpleLogger logger;

    /**
     * @param manager
     */
    public TaskFactory(XQSyncManager manager) {
        logger = manager.getLogger();
        copyPermissions = manager.getCopyPermissions();
        copyProperties = manager.getCopyProperties();

        outputSession = manager.getOutputConnection();
        outputPath = manager.getOutputPath();
        outputPackage = manager.getOutputPackage();

        readRoles = manager.getReadRoles();
        copyPermissions = manager.getCopyPermissions();
        copyProperties = manager.getCopyProperties();
        placeKeys = manager.getPlaceKeys();
    }

    /**
     * @param cs
     */
    private void configure(CallableSync cs) {
        cs.setLogger(logger);

        if (outputSession != null) {
            cs.setOutputSession(outputSession);
        } else if (outputPackage != null) {
            cs.setOutputPackage(outputPackage);
        } else if (outputPath != null) {
            cs.setOutputPath(outputPath);
        }

        if (readRoles != null)
            cs.setReadRoles(readRoles);

        if (placeKeys != null)
            cs.setPlaceKeys(placeKeys);
    }

    /**
     * @param file
     * @return
     * @throws IOException
     */
    public CallableSync newCallableSync(File file) throws IOException {
        CallableSync cs = new CallableSync(file, copyPermissions,
                copyProperties);
        configure(cs);
        return cs;
    }

    /**
     * @param uri
     * @return
     * @throws IOException
     * @throws XccException
     */
    public CallableSync newCallableSync(String uri) throws IOException, XccException {
        CallableSync cs;
        if (inputPackage != null) {
            cs = new CallableSync(inputPackage,
                    uri, copyPermissions, copyProperties);
        } else {
        Session session = (Session) inputConnection.newSession();
        cs = new CallableSync(session, uri,
                copyPermissions, copyProperties);
        }
        configure(cs);
        return cs;
    }

    /**
     * @param inputPackage
     */
    public void setInputPackage(XQSyncPackage inputPackage) {
        this.inputPackage = inputPackage;
    }

    public void setInputConnection(Connection inputConnection) {
        this.inputConnection = inputConnection;
    }

}
