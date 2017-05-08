/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c) 2006-2012 MarkLogic Corporation. All rights reserved.
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

import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import javax.transaction.xa.XAResource;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentbaseMetaData;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.ModuleSpawn;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.UserCredentials;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccException;
import com.marklogic.xcc.types.XSBoolean;
import com.marklogic.xcc.types.XSInteger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 */
public class Session implements com.marklogic.xcc.Session {

    /**
     *
     */
    public static final String XQUERY_VERSION_1_0_ML = "xquery version \"1.0-ml\";\n";

    private com.marklogic.xcc.Session session;

    private Connection conn;

    /**
     * @param session
     */
    public Session(Connection conn, com.marklogic.xcc.Session session) {
        this.conn = conn;
        this.session = session;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#getContentSource()
     */
    public Connection getContentSource() {
        return conn;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#getUserCredentials()
     */
    public UserCredentials getUserCredentials() {
        return session.getUserCredentials();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#getContentBaseName()
     */
    public String getContentBaseName() {
        return session.getContentBaseName();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#commit()
     */
    public void commit() throws RequestException{
        session.commit();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#rollback()
     */
    public void rollback() throws RequestException {
        session.rollback();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#close()
     */
    public void close() {
        session.close();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#isClosed()
     */
    public boolean isClosed() {
        return session.isClosed();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#submitRequest(com.marklogic.xcc.Request)
     */
    public ResultSequence submitRequest(Request request)
            throws RequestException {
        return session.submitRequest(request);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#newAdhocQuery(java.lang.String,
     * com.marklogic.xcc.RequestOptions)
     */
    public AdhocQuery newAdhocQuery(String queryText,
            RequestOptions options) {
        return session.newAdhocQuery(queryText, options);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#newAdhocQuery(java.lang.String)
     */
    public AdhocQuery newAdhocQuery(String queryText) {
        return session.newAdhocQuery(queryText);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#newModuleInvoke(java.lang.String,
     * com.marklogic.xcc.RequestOptions)
     */
    public ModuleInvoke newModuleInvoke(String moduleUri,
            RequestOptions options) {
        return session.newModuleInvoke(moduleUri, options);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#newModuleInvoke(java.lang.String)
     */
    public ModuleInvoke newModuleInvoke(String moduleUri) {
        return session.newModuleInvoke(moduleUri);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#newModuleSpawn(java.lang.String,
     * com.marklogic.xcc.RequestOptions)
     */
    public ModuleSpawn newModuleSpawn(String moduleUri,
            RequestOptions options) {
        return session.newModuleSpawn(moduleUri, options);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#newModuleSpawn(java.lang.String)
     */
    public ModuleSpawn newModuleSpawn(String moduleUri) {
        return session.newModuleSpawn(moduleUri);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#insertContent(com.marklogic.xcc.Content)
     */
    public void insertContent(Content content) throws RequestException {
        session.insertContent(content);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#insertContent(com.marklogic.xcc.Content[])
     */
    public void insertContent(Content[] content) throws RequestException {
        session.insertContent(content);
    }

    public List<RequestException> insertContentCollectErrors(Content[] content){
        return new ArrayList<RequestException>(0);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#getContentbaseMetaData()
     */
    public ContentbaseMetaData getContentbaseMetaData() {
        return session.getContentbaseMetaData();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.marklogic.xcc.Session#setDefaultRequestOptions(com.marklogic.xcc.
     * RequestOptions)
     */
    public void setDefaultRequestOptions(RequestOptions options) {
        session.setDefaultRequestOptions(options);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#getDefaultRequestOptions()
     */
    public RequestOptions getDefaultRequestOptions() {
        return session.getDefaultRequestOptions();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#getEffectiveRequestOptions()
     */
    public RequestOptions getEffectiveRequestOptions() {
        return session.getEffectiveRequestOptions();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#getCurrentServerPointInTime()
     */
    public BigInteger getCurrentServerPointInTime()
            throws RequestException {
        return session.getCurrentServerPointInTime();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#getLogger()
     */
    public Logger getLogger() {
        return session.getLogger();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#setLogger(java.util.logging.Logger)
     */
    public void setLogger(Logger logger) {
        session.setLogger(logger);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#setUserObject(java.lang.Object)
     */
    public void setUserObject(Object userObject) {
        session.setUserObject(userObject);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#getUserObject()
     */
    public Object getUserObject() {
        return session.getUserObject();
    }

    public boolean existsDocument(String _uri) throws XccException {
        String query = XQUERY_VERSION_1_0_ML
                + "declare variable $URI as xs:string external;\n"
                + "boolean(doc($URI))\n";
        AdhocQuery req = session.newAdhocQuery(query);
        req.setNewStringVariable("URI", _uri);
        ResultSequence result = session.submitRequest(req);
        return ((XSBoolean) (result.next().getItem()))
                .asPrimitiveBoolean();
    }

    public long getCount() throws XccException {
        String query = XQUERY_VERSION_1_0_ML + "xdmp:estimate(doc())";
        AdhocQuery req = session.newAdhocQuery(query);
        ResultSequence result = session.submitRequest(req);
        return ((XSInteger) (result.next().getItem())).asPrimitiveLong();
    }

    /**
     * @param _uri
     * @throws XccException
     */
    public void deleteDocument(String _uri) throws XccException {
        // ignore documents that do not exist
        String query = XQUERY_VERSION_1_0_ML
                + "declare variable $URI as xs:string external;\n"
                + "if (boolean(doc($URI)))\n"
                + "then xdmp:document-delete($URI) else ()\n";
        AdhocQuery req = session.newAdhocQuery(query);
        req.setNewStringVariable("URI", _uri);
        session.submitRequest(req);
    }

    /**
     * @param _uri
     * @throws XccException
     */
    public void deleteCollection(String _uri) throws XccException {
        String query = XQUERY_VERSION_1_0_ML
                + "declare variable $URI as xs:string external;\n"
                + "xdmp:collection-delete($URI)\n";
        AdhocQuery req = session.newAdhocQuery(query);
        req.setNewStringVariable("URI", _uri);
        session.submitRequest(req);
    }

    /**
     * @param remoteURI
     * @param string
     * @throws XccException
     */
    public void setDocumentProperties(String _uri, String _xmlString)
            throws XccException {
        // if an empty string is passed in,
        // properties will be set to empty sequence
        // this doesn't affect last-modified, though, if it's active
        // note that we go down two levels, to get the prop:properties children
        String query = XQUERY_VERSION_1_0_ML
                + "declare variable $URI as xs:string external;\n"
                + "declare variable $XML-STRING as xs:string external;\n"
                + "xdmp:document-set-properties($URI,\n"
                + "  xdmp:unquote($XML-STRING)/prop:properties/node() )\n";
        AdhocQuery req = session.newAdhocQuery(query);
        req.setNewStringVariable("URI", _uri);
        req.setNewStringVariable("XML-STRING", _xmlString);
        session.submitRequest(req);
    }

    /**
     * @param _names
     * @return
     * @throws XccException
     */
    public Map<String, BigInteger> getForestMap() throws XccException {
        return session.getContentbaseMetaData().getForestMap();
    }

    /**
     * @param _names
     * @return
     * @throws XccException
     */
    public BigInteger[] forestNamesToIds(String[] _names)
            throws XccException {
        if (_names == null) {
            return null;
        }

        Map<String, BigInteger> map = session.getContentbaseMetaData()
                .getForestMap();
        List<BigInteger> list = new ArrayList<BigInteger>();
        for (int i = 0; i < _names.length; i++) {
            list.add(map.get(_names[i]));
        }
        return list.toArray(new BigInteger[0]);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.xcc.Session#getConnectionUri()
     */
    public URI getConnectionUri() {
        return session.getConnectionUri();
    }

    /**
     * <p>
     * Sets the transaction mode to the given value. The initial value is
     * TransactionMode.AUTO.
     * </p>
     * <p>
     * If the transaction mode is TransactionMode.AUTO, a new transaction is created for
     * every request, and committed (or rolled back) at the end of that request.
     * The type of transaction created is determined automatically by query analysis.
     * </p>
     * <p>
     * If transaction mode is TransactionMode.QUERY or TransactionMode.UPDATE, requests
     * are grouped under transactions bounded by calls to Session.commit() or Session.rollback().
     * If transaction mode is TransactionMode.QUERY, then a read-only query transaction is created
     * to group requests. If transaction mode is TransactionMode.UPDATE, then a locking update
     * transaction is created. If an updating request is executed under a read-only
     * TransactionMode.QUERY transaction, a RequestException is thrown.
     * </p>
     * <p>
     * Calling setTransactionMode() while a transaction is active has no effect on the current
     * transaction.
    * </p>
     * @param mode The new transaction mode
     */
    public void setTransactionMode(TransactionMode mode) {
        session.setTransactionMode(mode);
    }

    /**
     * Get the current transaction mode.
     * 
     * @return The current transaction mode setting.
     */
    public TransactionMode getTransactionMode() {
        return session.getTransactionMode();
    }

    /**
     * Sets the timeout for transactions
     * @param seconds The number of seconds before the transaction times out
     * @throws RequestEception
     *             If there is a problem communicating with the server.
     */
    public void setTransactionTimeout(int seconds) throws RequestException {
        session.setTransactionTimeout(seconds);
    }

    /**
     * Get the current transaction timeout.
     *
     * @return The current transaction timeout setting.
     * @throws RequestException
     *             If there is a problem communicating with the server.
     */
    public int getTransactionTimeout() throws RequestException {
        return session.getTransactionTimeout();
    }

    /**
     * <p>
     * Returns an instance of the XAResource interface, specific to this Session object.
     * This can be used to take part in JTA distributed transactions using an implementation of
     * javax.transaction.TransactionManager.
     * </p>
     *
     * @return The XAResource object.
     */
    public XAResource getXAResource() {
        return session.getXAResource();
    }

    public int getCachedTxnTimeout() {
        throw new java.lang.UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
