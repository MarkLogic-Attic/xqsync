/**
 * Copyright (c) 2006-2008 Mark Logic Corporation. All rights reserved.
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
    public static final String XQUERY_VERSION_0_9_ML = "xquery version \"0.9-ml\"\n";

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
     * @see com.marklogic.xcc.Session#setAutoCommit(boolean)
     */
    public void setAutoCommit(boolean newValue) {
        session.setAutoCommit(newValue);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.xcc.Session#getAutoCommit()
     */
    public boolean getAutoCommit() {
        return session.getAutoCommit();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.xcc.Session#commit()
     */
    public void commit() {
        session.commit();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.xcc.Session#rollback()
     */
    public void rollback() {
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
        String query = XQUERY_VERSION_0_9_ML
                + "define variable $URI as xs:string external\n"
                + "boolean(doc($URI))\n";
        AdhocQuery req = session.newAdhocQuery(query);
        req.setNewStringVariable("URI", _uri);
        ResultSequence result = session.submitRequest(req);
        return ((XSBoolean) (result.next().getItem()))
                .asPrimitiveBoolean();
    }

    public long getCount() throws XccException {
        String query = XQUERY_VERSION_0_9_ML + "xdmp:estimate(doc())";
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
        String query = XQUERY_VERSION_0_9_ML
                + "define variable $URI as xs:string external\n"
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
        String query = XQUERY_VERSION_0_9_ML
                + "define variable $URI as xs:string external\n"
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
        String query = XQUERY_VERSION_0_9_ML
                + "define variable $URI as xs:string external\n"
                + "define variable $XML-STRING as xs:string external\n"
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
    @SuppressWarnings("unchecked")
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
}
