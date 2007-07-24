/*
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
package com.marklogic.ps;

import java.net.URI;
import java.util.logging.Logger;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class Connection implements ContentSource {

    private URI uri;

    private ContentSource cs;

    /**
     * @param _uri
     * @throws XccException
     */
    public Connection(URI _uri) throws XccException {
        // detect bad URIs, since the JVM allows them
        if (null == _uri.getHost()) {
            throw new UnimplementedFeatureException(
                    "bad URI: cannot parse host from " + _uri);
        }
        uri = _uri;
        cs = ContentSourceFactory.newContentSource(uri);
    }

    /**
     * @return
     */
    public URI getUri() {
        return uri;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.xcc.ContentSource#newSession()
     */
    public Session newSession() {
        return new com.marklogic.ps.Session(this, cs.newSession());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.xcc.ContentSource#newSession(java.lang.String)
     */
    public Session newSession(String contentbaseId) {
        return new com.marklogic.ps.Session(this, cs
                .newSession(contentbaseId));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.xcc.ContentSource#newSession(java.lang.String,
     *      java.lang.String)
     */
    public Session newSession(String userName, String password) {
        return new com.marklogic.ps.Session(this, cs.newSession(userName,
                password));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.xcc.ContentSource#newSession(java.lang.String,
     *      java.lang.String, java.lang.String)
     */
    public Session newSession(String userName, String password,
            String contentbaseId) {
        return new com.marklogic.ps.Session(this, cs.newSession(userName,
                password, contentbaseId));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.xcc.ContentSource#getDefaultLogger()
     */
    public Logger getDefaultLogger() {
        return cs.getDefaultLogger();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.xcc.ContentSource#setDefaultLogger(java.util.logging.Logger)
     */
    public void setDefaultLogger(Logger logger) {
        cs.setDefaultLogger(logger);
    }

}
