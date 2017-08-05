/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c)2004-2017 MarkLogic Corporation
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
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.exceptions.XccException;
import com.marklogic.xcc.spi.ConnectionProvider;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class Connection implements ContentSource {

    protected URI[] uri;

    protected ContentSource[] cs;

    protected volatile int count = 0;

    private Object securityOptionsMutex = new Object();

    protected static SecurityOptions securityOptions = null;

    /**
     * @param _uri
     * @throws XccException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    public Connection(URI _uri) throws XccException,
            KeyManagementException, NoSuchAlgorithmException {
        init(new URI[] { _uri });
    }

    /**
     * @param _uris
     * @param _uris
     * @throws XccException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    public Connection(URI[] _uris) throws XccException,
            KeyManagementException, NoSuchAlgorithmException {
        init(_uris);
    }

    /**
     * @param _uris
     * @throws XccConfigException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    private void init(URI[] _uris) throws XccConfigException,
            KeyManagementException, NoSuchAlgorithmException {
        if (null == _uris || 1 > _uris.length) {
            throw new NullPointerException("must supply uris");
        }
        // detect bad URIs, since the JVM allows them
        uri = new URI[_uris.length];
        cs = new ContentSource[_uris.length];
        for (int i = 0; i < _uris.length; i++) {
            if (null == _uris[i].getHost()) {
                throw new UnimplementedFeatureException(
                        "bad URI: cannot parse host from " + _uris[i]);
            }
            uri[i] = _uris[i];
            // support SSL
            boolean ssl = uri[i].getScheme().equals("xccs");
            cs[i] = ssl ? ContentSourceFactory.newContentSource(uri[i],
                    getSecurityOptions()) : ContentSourceFactory
                    .newContentSource(uri[i]);
        }
    }

    /**
     * @return
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    private SecurityOptions getSecurityOptions()
            throws KeyManagementException, NoSuchAlgorithmException {
        if (null != securityOptions) {
            return securityOptions;
        }
        synchronized (securityOptionsMutex) {
            if (null != securityOptions) {
                return securityOptions;
            }
            securityOptions = newTrustAnyoneOptions();
            return securityOptions;
        }
    }

    /**
     * @return
     */
    public URI getUri() {
        return uri[count++ % uri.length];
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.ContentSource#newSession()
     */
    public Session newSession() {
        return new com.marklogic.ps.Session(this, getContentSource()
                .newSession());
    }

    /**
     * @return true if basic authentication will be attempted preemptively, false otherwise.
     */
    public boolean isAuthenticationPreemptive() {
        return getContentSource().isAuthenticationPreemptive();
    }

    /**
     * <p>Sets whether basic authentication should be attempted preemptively, default is false.</p>
     *
     * <p>Preemptive authentication can reduce the overhead of making connections to servers that accept
     * basic authentication by eliminating the challenge-response interaction otherwise required.</p>
     *
     * <p>Note that misuse of preemptive authentication entails potential security risks, and under most
     * circumstances the credentials used to authenticate will be cached after the first connection.  To
     * avoid creating the illusion that credentials are protected, connections to a server requiring digest
     * authentication will not be retried if this flag is set.</p>
     *
     * @param value true if basic authentication should be attempted preemptively, false otherwise.
     */
    public void setAuthenticationPreemptive(boolean value) {
        getContentSource().setAuthenticationPreemptive(value);
    }

    /**
     * @return The ConnectionProvider used to construct this ContentSource.
     */
    public ConnectionProvider getConnectionProvider() {
        return getContentSource().getConnectionProvider();
    }

    /**
     * @return
     */
    public synchronized ContentSource getContentSource() {
        // this may not need synchronized, since count is volatile,
        // but no one should call this function frequently
        return cs[count++ % cs.length];
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.ContentSource#newSession(java.lang.String)
     */
    public Session newSession(String contentbaseId) {
        return new com.marklogic.ps.Session(this, getContentSource()
                .newSession(contentbaseId));
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.ContentSource#newSession(java.lang.String,
     * java.lang.String)
     */
    public Session newSession(String userName, String password) {
        return new com.marklogic.ps.Session(this, getContentSource()
                .newSession(userName, password));
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.ContentSource#newSession(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    public Session newSession(String userName, String password,
            String contentbaseId) {
        return new com.marklogic.ps.Session(this, getContentSource()
                .newSession(userName, password, contentbaseId));
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.ContentSource#getDefaultLogger()
     */
    public Logger getDefaultLogger() {
        return getContentSource().getDefaultLogger();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.marklogic.xcc.ContentSource#setDefaultLogger(java.util.logging.Logger
     * )
     */
    public void setDefaultLogger(Logger logger) {
        getContentSource().setDefaultLogger(logger);
    }

    protected static SecurityOptions newTrustAnyoneOptions()
            throws KeyManagementException, NoSuchAlgorithmException {
        TrustManager[] trust = new TrustManager[] { new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }

            /**
             * @throws CertificateException
             */
            public void checkClientTrusted(X509Certificate[] certs,
                    String authType) throws CertificateException {
                // no exception means it's okay
            }

            /**
             * @throws CertificateException
             */
            public void checkServerTrusted(X509Certificate[] certs,
                    String authType) throws CertificateException {
                // no exception means it's okay
            }
        } };

        SSLContext sslContext = SSLContext.getInstance("SSLv3");
        sslContext.init(null, trust, null);
        return new SecurityOptions(sslContext);
    }
}
