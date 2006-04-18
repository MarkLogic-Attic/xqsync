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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import com.marklogic.xdbc.XDBCException;
import com.marklogic.xdbc.XDBCResultSequence;
import com.marklogic.xdbc.XDBCStatement;
import com.marklogic.xdbc.XDBCXName;
import com.marklogic.xdbc.XDBCXQueryException;
import com.marklogic.xdbc.XDBCXQueryRuntimeException;
import com.marklogic.xdmp.XDMPConnection;
import com.marklogic.xdmp.XDMPDocInsertStream;
import com.marklogic.xdmp.XDMPDocOptions;
import com.marklogic.xdmp.XDMPPermission;
import com.marklogic.xdmp.util.XDMPAuthenticator;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 *
 */
public class Connection {
    protected static SimpleLogger logger = SimpleLogger.getSimpleLogger();

    private XDMPConnection conn;

    private String host, user, password;

    private int port = -1;

    private int mergingThrottle = 0;

    private int sleepTime = 500;

    private boolean commitSupported = false;

    private static Properties properties;

    public static final int DOC_FORMAT_XML = XDMPDocInsertStream.XDMP_DOC_FORMAT_XML;

    public static final int DOC_FORMAT_NONE = XDMPDocInsertStream.XDMP_DOC_FORMAT_NONE;

    public static final int DOC_FORMAT_BINARY = XDMPDocInsertStream.XDMP_DOC_FORMAT_BINARY;

    public static final int DOC_FORMAT_TEXT = XDMPDocInsertStream.XDMP_DOC_FORMAT_TEXT;

    public Connection(String _host, int _port, String _user, String _password) {
        host = _host;
        port = _port;
        user = _user;
        password = _password;
    }

    /**
     * @param connString
     * @throws XDBCException
     */
    public Connection(String connString) throws XDBCException {
        logger.finer(connString);
        if (connString == null) {
            throw new XDBCException("null connection string");
        }

        // parse [user:password@]host:port into its parts
        String[] connParams = connString.split("[@:]");
        if (connParams.length == 4) {
            user = connParams[0];
            password = connParams[1];
            host = connParams[2];
            port = Integer.parseInt(connParams[3]);
            logger.finer("connection string parsed to: "
                    + buildConnectionString(host, port, user, password));
        } else if (connParams.length == 2) {
            host = connParams[0];
            port = Integer.parseInt(connParams[1]);
            logger.finer("connection string parsed to: "
                    + buildConnectionString(host, port));
        } else {
            throw new XDBCException("bad connection string: " + connString);
        }
    }

    /**
     * @return
     */
    private String buildConnectionString() {
        return buildConnectionString(host, port, user, password);
    }

    /**
     * @param _host
     * @param _port
     * @return
     */
    private String buildConnectionString(String _host, int _port) {
        return buildConnectionString(_host, _port, null, null);
    }

    /**
     * @param _host
     * @param _port
     * @param _user
     * @param _password
     * @return
     */
    public static String buildConnectionString(String _host, String _port,
            String _user, String _password) {
        return buildConnectionString(_host, Integer.parseInt(_port), _user,
                _password);
    }

    /**
     * @param _host
     * @param _port
     * @param _user
     * @param _password
     * @return
     */
    public static String buildConnectionString(String _host, int _port,
            String _user, String _password) {
        String rval = _host + ":" + _port;
        if (_user != null && _password != null)
            rval = _user + ":" + _password + "@" + rval;
        return rval;
    }

    public boolean checkFile(String _uri) throws XDBCException {
        String query = "define variable $URI as xs:string external\n"
                + "exists(doc($URI)/node())\n";
        Hashtable vars = new Hashtable();
        vars.put(new XDBCXName("", "URI"), _uri);

        XDBCResultSequence result = null;
        boolean exists = false;
        try {
            result = executeQuery(query, vars);
            if (!result.hasNext())
                throw new XDBCException("unexpected null result");

            result.next();
            if (result.getItemType() != XDBCResultSequence.XDBC_Boolean)
                throw new XDBCException("unexpected result: "
                        + result.getItemType());

            exists = result.get_boolean();
        } finally {
            if (result != null && !result.isClosed())
                result.close();
        }
        return exists;
    }

    public long getCount() throws XDBCException {
        String query = "xdmp:estimate(input())";
        XDBCResultSequence result = null;
        try {
            result = executeQuery(query);
            if (!result.hasNext())
                throw new XDBCException("unexpected null result");

            result.next();
            if (result.getItemType() != XDBCResultSequence.XDBC_Integer)
                throw new XDBCException("unexpected result: "
                        + result.getItemType());

            int count = result.get_int();
            result.close();
            return count;
        } finally {
            try {
                if (result != null && !result.isClosed())
                    result.close();
            } catch (XDBCException e) {
            }
        }
    }

    public XDMPConnection getConnection() throws XDBCException {
        if (conn == null || conn.isClosed()) {
            logger.fine("new connection: " + buildConnectionString());
            if (host == null || port < 1)
                throw new XDBCException("must supply valid host and port");

            if (user != null && password != null) {
                logger.finer("getConnection: " + user + ":" + password + "@"
                        + host + ":" + port);
                XDMPAuthenticator.setDefault(new XDMPAuthenticator(user,
                        password));
            } else {
                logger.finer("getConnection: " + host + ":" + port);
            }
            conn = new XDMPConnection(host, port);
        }
        return conn;
    }

    public XDBCResultSequence executeQuery(String _query, Map _externals)
            throws XDBCException {
        return executeQuery(_query, _externals, true);
    }

    /**
     * @param query
     * @return
     * @throws XDBCException
     */
    public XDBCResultSequence executeQuery(String _query) throws XDBCException {
        return executeQuery(_query, true);
    }

    public XDBCResultSequence executeQuery(String _query, Map _externals,
            boolean _retry) throws XDBCException {
        XDBCStatement stmt = createStatement();
        marshalExternalVariables(stmt, _externals);
        return executeQuery(stmt, _query);
    }

    /**
     * @return
     * @throws XDBCException
     */
    private XDBCStatement createStatement() throws XDBCException {
        if (conn == null || conn.isClosed())
            conn = getConnection();

        return conn.createStatement();
    }

    /**
     * @param _stmt
     * @param _query
     * @return
     * @throws XDBCException
     */
    protected XDBCResultSequence executeQuery(XDBCStatement _stmt, String _query)
            throws XDBCException {
        return executeQuery(_stmt, _query, true);
    }

    public XDBCResultSequence executeQuery(String _query, boolean _retry)
            throws XDBCException {
        XDBCStatement stmt = createStatement();
        return executeQuery(stmt, _query, _retry);
    }

    protected XDBCResultSequence executeQuery(XDBCStatement _stmt,
            String _query, boolean _retry) throws XDBCException {
        logger.finer("executeQuery: " + _query);
        if (_query == null || _query.trim().equals(""))
            return null;
        if (conn == null || conn.isClosed())
            conn = getConnection();

        // handle retryable exceptions transparently
        // examples: XMDP-DEADLOCK
        XDBCResultSequence rs = null;
        while (true) {
            try {
                rs = _stmt.executeQuery(_query);
                // caller *must* close the resultsequence
                return rs;
            } catch (XDBCXQueryRuntimeException e) {
                try {
                    if (rs != null && !rs.isClosed())
                        rs.close();
                    if (_stmt != null && !_stmt.isClosed())
                        _stmt.close();
                } catch (XDBCException xe) {
                }
                if (_retry && e.getRetryable()) {
                    logger.logException("retryable exception: "
                            + e.getLocalizedMessage(), e);
                    logger.warning("retryable exception - will try again: "
                            + e.getLocalizedMessage());
                    continue;
                } else {
                    // reconnect();
                    throw e;
                }
            } catch (XDBCXQueryException e) {
                try {
                    if (rs != null && !rs.isClosed())
                        rs.close();
                    if (_stmt != null && !_stmt.isClosed())
                        _stmt.close();
                } catch (XDBCException xe) {
                }
                if (_retry && e.getRetryable()) {
                    logger.logException("retryable exception: "
                            + e.getLocalizedMessage(), e);
                    logger.warning("retryable exception - will try again: "
                            + e.getLocalizedMessage());
                    continue;
                } else {
                    // no need to reconnect() for an xquery-level error
                    throw e;
                }
            } catch (XDBCException e) {
                try {
                    if (rs != null && !rs.isClosed())
                        rs.close();
                    if (_stmt != null && !_stmt.isClosed())
                        _stmt.close();
                } catch (XDBCException xe) {
                }
                // reconnect();
                throw e;
            }
        }
    }

    /**
     * @param query
     * @return
     */
    public XDBCResultSequence invoke(String _module, String _database,
            Map _args, boolean _retry) throws XDBCException {
        logger.finer("invoke: " + _module);
        if (_module == null || _module.trim().equals(""))
            return null;
        if (conn == null || conn.isClosed())
            conn = getConnection();

        // handle retryable exceptions transparently
        // examples: XMDP-DEADLOCK
        XDBCStatement stmt = createStatement();
        XDBCResultSequence rs = null;
        while (true) {
            try {
                XDBCXName[] args = marshalExternalVariables(stmt, _args);
                rs = stmt.invoke(_module, _database, args);
                // caller *must* close the resultsequence
                return rs;
            } catch (XDBCXQueryRuntimeException e) {
                try {
                    if (rs != null && !rs.isClosed())
                        rs.close();
                    if (stmt != null && !stmt.isClosed())
                        stmt.close();
                } catch (XDBCException xe) {
                }
                if (_retry && e.getRetryable()) {
                    logger.logException("retryable exception: "
                            + e.getLocalizedMessage(), e);
                    logger.warning("retryable exception - will try again: "
                            + e.getLocalizedMessage());
                    continue;
                } else {
                    // reconnect();
                    throw e;
                }
            } catch (XDBCXQueryException e) {
                try {
                    if (rs != null && !rs.isClosed())
                        rs.close();
                    if (stmt != null && !stmt.isClosed())
                        stmt.close();
                } catch (XDBCException xe) {
                }
                if (_retry && e.getRetryable()) {
                    logger.logException("retryable exception: "
                            + e.getLocalizedMessage(), e);
                    logger.warning("retryable exception - will try again: "
                            + e.getLocalizedMessage());
                    continue;
                } else {
                    // reconnect();
                    throw e;
                }
            } catch (XDBCException e) {
                try {
                    if (rs != null && !rs.isClosed())
                        rs.close();
                    if (stmt != null && !stmt.isClosed())
                        stmt.close();
                } catch (XDBCException xe) {
                }
                // reconnect();
                throw e;
            }
        }
    }

    /**
     * @param _args
     * @param argsCount
     * @return
     * @throws XDBCException
     */
    private XDBCXName[] marshalExternalVariables(XDBCStatement _stmt, Map _args)
            throws XDBCException {
        if (_args == null)
            return null;

        int argsCount = _args.size();
        if (argsCount < 1)
            return null;

        XDBCXName[] args = null;

        _stmt.clearVariables();

        // marshall the arguments
        args = new XDBCXName[argsCount];
        Object key = null;
        XDBCXName name;
        Iterator iter = _args.keySet().iterator();
        int i = 0;
        while (iter.hasNext()) {
            // keys may be XDBCXName or String
            key = iter.next();
            if (key instanceof String) {
                name = new XDBCXName((String) key);
            } else if (key instanceof XDBCXName) {
                name = (XDBCXName) key;
            } else {
                throw new XDBCException("cannot cast to XDBCXName: " + key);
            }
            args[i++] = name;
            setValue(_stmt, name, _args.get(key));
        }

        return args;
    }

    public XDBCResultSequence invoke(String _module, String _database, Map _args)
            throws XDBCException {
        return invoke(_module, _database, _args, true);
    }

    public XDBCResultSequence invoke(String _module, String _database)
            throws XDBCException {
        return invoke(_module, _database, null, true);
    }

    public XDBCResultSequence invoke(String _module, String _database,
            boolean _retry) throws XDBCException {
        return invoke(_module, _database, null, _retry);
    }

    public XDBCResultSequence invoke(String _module, Map _args)
            throws XDBCException {
        return invoke(_module, null, _args, true);
    }

    public XDBCResultSequence invoke(String _module) throws XDBCException {
        return invoke(_module, null, null, true);
    }

    /**
     * @param key
     * @param value
     * @throws XDBCException
     */
    private void setValue(XDBCStatement _stmt, XDBCXName _key, Object _value)
            throws XDBCException {
        // NOTE: caller must have called getStatement() first

        if (_value == null) {
            _stmt.setNull(_key);
            logger.finest("" + _key + " => null");
        } else {
            // handle all types that won't work as untypedAtomic:
            // what kind of object is this value?
            String valueType = _value.getClass().getName();
            logger.finest("" + _key + " => " + _value + " as " + valueType);

            if (valueType.equals("java.lang.Boolean")) {
                _stmt.setBoolean(_key, ((Boolean) _value).booleanValue());
            } else if (valueType.equals("java.math.BigInteger")) {
                _stmt.setInteger(_key, (BigInteger) _value);
            } else if (valueType.equals("java.lang.Integer")) {
                _stmt.setInteger(_key, ((Integer) _value).intValue());
            } else if (valueType.equals("java.math.BigDecimal")) {
                _stmt.setDecimal(_key, (BigDecimal) _value);

            } else if (valueType.equals("java.lang.Double")) {
                _stmt.setDouble(_key, ((Double) _value).doubleValue());

            } else if (valueType.equals("java.lang.String")) {
                _stmt.setString(_key, (String) _value);

            } else if (valueType.equals("java.util.Date")) {
                _stmt.setDateTime(_key, (Date) _value);

            } else {
                _stmt.setUntypedAtomic(_key, String.valueOf(_value));
            }
        }
    }

    /**
     *
     */
    public void commit() throws XDBCException {
        if (commitSupported && conn != null && !conn.isClosed())
            conn.commit();
    }

    /**
     * @return
     */
    public int getMergingThrottle() {
        return mergingThrottle;
    }

    /**
     * @param _throttle
     */
    public void setMergingThrottle(int _throttle) {
        mergingThrottle = _throttle;
    }

    /**
     * @return
     */
    public int getSleepTime() {
        return sleepTime;
    }

    /**
     * @param _sleepTime
     */
    public void setSleepTime(int _sleepTime) {
        sleepTime = _sleepTime;
    }

    /**
     *
     */
    public void close() {
        try {
            if (conn != null && !conn.isClosed())
                conn.close();

            conn = null;
        } catch (XDBCException e) {
            e.printStackTrace();
        }
    }

    /**
     * @return
     * @throws XDBCException
     */
    public boolean isClosed() throws XDBCException {
        // don't throw NullPointerExceptions
        if (conn == null)
            return true;
        return conn.isClosed();
    }

    public void loadFile(String filePath, String uri) throws XDBCException,
            IOException {
        loadFile(filePath, uri, null);
    }

    public void loadFile(String filePath, String uri, String[] collections)
            throws XDBCException, IOException {
        loadFile(filePath, uri, collections, null, true,
                XDMPDocInsertStream.XDMP_DOC_FORMAT_NONE);
    }

    public void loadFile(String filePath, String uri, int format,
            String[] collections) throws XDBCException, IOException {
        loadFile(filePath, uri, collections, null, true, format);
    }

    /**
     * @param outputFilePath
     * @param tempUri
     * @param object
     * @param collections
     * @throws XDBCException
     * @throws XDBCException
     * @throws IOException
     * @throws IOException
     */
    public void loadFile(String filePath, String uri, String[] collections,
            XDMPPermission[] permissions, boolean resolveEntities, int format)
            throws XDBCException, IOException {
        logger.finer("loading " + filePath + " as " + uri);
        FileInputStream is = null;
        is = new FileInputStream(filePath);
        insertDocument(uri, is, collections, permissions, resolveEntities,
                format);
        is.close();
    }

    public void insertDocument(String uri, String xmlString,
            String[] collections) throws XDBCException, IOException {
        Reader r = new StringReader(xmlString);
        insertDocument(uri, r, collections);
        r.close();
    }

    public void insertDocument(String uri, String xmlString,
            String[] collections, XDMPPermission[] permissions,
            boolean resolveEntities, int format) throws XDBCException,
            IOException {
        Reader r = new StringReader(xmlString);
        insertDocument(uri, r, collections, permissions, resolveEntities,
                format);
        r.close();
    }

    public void insertDocument(String uri, Reader r, String[] collections)
            throws XDBCException, IOException {
        insertDocument(uri, r, collections, null, false,
                XDMPDocInsertStream.XDMP_DOC_FORMAT_NONE);
    }

    public void insertDocument(String uri, Reader reader, String[] collections,
            XDMPPermission[] permissions, boolean resolveEntities, int format)
            throws XDBCException, IOException {
        logger.finer("loading " + reader + " as " + uri);
        XDMPDocInsertStream os = null;
        try {
            getConnection();
            // defaults
            int quality = 0;
            int repair = XDMPDocInsertStream.XDMP_ERROR_CORRECTION_NONE;
            String namespace = null;
            String[] placeKeys = null;
            String language = null;
            XDMPDocOptions docOpts = new XDMPDocOptions(Locale.getDefault(),
                    resolveEntities, permissions, collections, quality,
                    namespace, repair, placeKeys, format, language);
            os = conn.openDocInsertStream(uri, docOpts);
            Utilities.copy(reader, os);
        } catch (XDBCException e) {
            throw (e);
        } catch (IOException e) {
            throw (e);
        } finally {
            if (os != null) {
                os.flush();
                os.commit();
                os.close();
            }
            // caller must close reader
        }
    }

    public void insertDocument(String uri, InputStream is,
            String[] collections, XDMPPermission[] permissions,
            boolean resolveEntities, int format) throws XDBCException,
            IOException {
        logger.finer("loading " + is + " as " + uri);
        XDMPDocInsertStream os = null;
        try {
            getConnection();
            // defaults
            int quality = 0;
            int repair = XDMPDocInsertStream.XDMP_ERROR_CORRECTION_NONE;
            String namespace = null;
            String[] placeKeys = null;
            String language = null;
            XDMPDocOptions docOpts = new XDMPDocOptions(Locale.getDefault(),
                    resolveEntities, permissions, collections, quality,
                    namespace, repair, placeKeys, format, language);
            os = conn.openDocInsertStream(uri, docOpts);
            Utilities.copy(is, os);
        } catch (XDBCException e) {
            throw (e);
        } catch (IOException e) {
            throw (e);
        } finally {
            if (os != null) {
                os.flush();
                os.commit();
                os.close();
            }
            // caller must close is
        }
    }

    /**
     * @param uri
     * @throws XDBCException
     */
    public void deleteDocument(String uri) throws XDBCException {
        // ignore documents that do not exist
        String query = "define variable $uri as xs:string external\n"
                + "if (exists(doc($uri))) then xdmp:document-delete($uri) else ()\n";
        Map externs = new HashMap(1);
        externs.put(new XDBCXName("", "uri"), uri);
        XDBCResultSequence rs = null;
        try {
            rs = executeQuery(query, externs);
        } finally {
            if (rs != null && !rs.isClosed())
                rs.close();
        }
    }

    /**
     * @param uri
     * @throws XDBCException
     */
    public void deleteCollection(String uri) throws XDBCException {
        String query = "define variable $uri as xs:string external\n"
                + "xdmp:collection-delete($uri)\n";
        Map externs = new HashMap(1);
        externs.put(new XDBCXName("", "uri"), uri);
        XDBCResultSequence rs = null;
        try {
            rs = executeQuery(query, externs);
        } finally {
            if (rs != null && !rs.isClosed())
                rs.close();
        }
    }

    /**
     * @param _properties
     */
    public static void setProperties(Properties _properties) {
        properties = _properties;
        logger.configureLogger(properties);
    }

    /**
     * @param remoteURI
     * @param string
     * @throws XDBCException
     */
    public void setDocumentProperties(String _uri, String _xmlString)
            throws XDBCException {
        // if an empty string is passed in,
        // properties will be set to empty sequence
        // this doesn't affect last-modified, though, if it's active
        String query = "define variable $uri as xs:string external\n"
                + "define variable $xml-string as xs:string external\n"
                + "xdmp:document-set-properties($uri, xdmp:unquote($xml-string)/node())";
        Map externals = new Hashtable(2);
        externals.put("uri", _uri);
        externals.put("xml-string", _xmlString);
        logger.finest(_uri + ": " + _xmlString);
        XDBCResultSequence rs = executeQuery(query, externals);
        rs.close();
    }

    /**
     * @return
     */
    public String getConnectionString() {
        return buildConnectionString();
    }

    /**
     * @param uri
     * @param permissions
     * @param collections
     * @param quality
     * @throws XDBCException
     */
    public void setDocumentMetaData(String uri, XDMPPermission[] permissions,
            String[] collections, int quality) throws XDBCException {
        // for efficiency, set all the metadata at once
        String query = "define variable $uri as xs:string external\n"
                + "define variable $collections as xs:string* external\n"
                + "define variable $roles as xs:string* external\n"
                + "define variable $capabilities as xs:string* external\n"
                + "define variable $quality as xs:integer external\n"
                + "if ($quality ne -1) then xdmp:document-set-quality($uri, $quality) else (),\n"
                + "if (exists($collections)) then xdmp:document-set-collections($uri, $collections) else (),\n"
                + "if (exists($roles) and count($roles) eq count($capabilities))\n"
                + "then xdmp:document-set-permissions(\n"
                + "$uri, for $r at $x in $roles return xdmp:permission($r, $capabilities[$x])\n"
                + ")\n" + "else ()\n";
        Map externals = new Hashtable(4);
        externals.put("uri", uri);
        externals.put("quality", new Integer(quality));
        externals.put("collections", collections);
        // tricky: we XDBCStatement doesn't have setPermission()
        String[] roles = new String[permissions.length];
        String[] capabilities = new String[permissions.length];
        int capability;
        for (int i = 0; i < permissions.length; i++) {
            roles[i] = permissions[i].getRole();
            capability = permissions[i].getCapability();
            switch (capability) {
            case XDMPPermission.READ:
                capabilities[i] = "read";
                break;
            case XDMPPermission.INSERT:
                capabilities[i] = "insert";
                break;
            case XDMPPermission.UPDATE:
                capabilities[i] = "update";
                break;
            default:
                throw new XDBCException("unrecognized permission capability: "
                        + capability);
            }
        }
        externals.put("roles", roles);
        externals.put("capabilities", capabilities);
        XDBCResultSequence rs = executeQuery(query, externals);
        if (rs != null)
            rs.close();
    }

    public XDMPDocInsertStream openDocInsertStream(String _uri)
            throws XDBCException {
        return getConnection().openDocInsertStream(_uri);
    }

    public XDMPDocInsertStream openDocInsertStream(String _uri,
            XDMPDocOptions _docOpts) throws XDBCException {
        return getConnection().openDocInsertStream(_uri, _docOpts);
    }

    /**
     * @param forestNames
     * @return
     * @throws XDBCException
     */
    public List forestNamesToIds(String[] forestNames) throws XDBCException {
        List list = new ArrayList();
        // whitespace is ok, since forest-names can't contain whitespace
        String query = "define variable forest-names-string as xs:string external\n"
                + "for $fn in tokenize($forest-names-string, '\\s+')[. ne '']\n"
                + "return string(xdmp:forest($fn))\n";
        Map args = new HashMap(1);
        args.put("forest-names-string", Utilities.join(forestNames, " "));

        XDBCResultSequence rs = null;

        rs = executeQuery(query, args);
        while (rs.hasNext()) {
            rs.next();
            list.add(rs.get_String());
        }
        if (rs != null) {
            rs.close();
        }

        return list;
    }

}
