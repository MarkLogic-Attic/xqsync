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
package com.marklogic.ps.xqsync;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Map;

import org.w3c.dom.Node;

import com.marklogic.ps.Connection;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.xdbc.XDBCException;
import com.marklogic.xdbc.XDBCResultSequence;
import com.marklogic.xdbc.XDBCXName;
import com.marklogic.xdbc.XDBCXQueryException;
import com.marklogic.xdmp.XDMPDocInsertStream;
import com.marklogic.xdmp.XDMPPermission;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 *
 */
public class XQSyncDocument {

    public static final String METADATA_EXT = ".metadata";

    public static final String METADATA_REGEX = "^.+\\"
            + XQSyncDocument.METADATA_EXT + "$";

    private byte[] contentBytes;

    private XQSyncDocumentMetadata metadata;

    private boolean copyPermissions = true;

    private boolean copyProperties = true;

    private SimpleLogger logger = null;

    /**
     * @param uri
     * @param _conn
     * @throws XDBCException
     * @throws IOException
     */
    public XQSyncDocument(String _uri, Connection _conn,
            boolean _copyPermissions, boolean _copyProperties,
            int _connMajorVersion) throws XDBCException, IOException {
        if (_uri == null)
            throw new XDBCException("null uri");

        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;

        // easy to distinguish the result-sets: metadata, data, properties
        // first is node-kind
        // then collection strings (if present)
        // then permission nodes (if present)
        // then quality integer (default 0)
        // then the document-node
        // then property node (if present)
        // one wrinkle: get-permissions() returns sec:permission/sec:role-id,
        // but our callers need the role-name! this gets ugly...
        // must wrap the list of permissions so we can pass it...
        //
        // normally I'd put this code in a module,
        // but I want this program to be self-contained
        String query = "define variable $uri as xs:string external\n"
                + "node-kind(doc($uri)/node()),\n"
                + "xdmp:document-get-collections($uri),\n";

        // use node for permissions, since we walk the tree
        if (copyPermissions)
            query += "let $list := xdmp:document-get-permissions($uri)\n"
                    + "let $query := concat(\n"
                    + (_connMajorVersion > 2 ? "' import module ''http://marklogic.com/xdmp/security'' at ''/MarkLogic/security.xqy''',\n"
                            : "' import module ''http://marklogic.com/xdmp/security'' at ''/security.xqy''',\n")
                    + "' define variable $list as element(sec:permissions) external',\n"
                    + "' for $p in $list/sec:permission',\n"
                    + "' return element sec:permission {',\n"
                    + "'  $p/node(), sec:get-role-names($p/sec:role-id)',\n"
                    + "' }'\n"
                    + ")\n"
                    + "return if (empty($list)) then () else\n"
                    + "xdmp:eval-in(\n"
                    + "  $query, xdmp:security-database(),\n"
                    + "  (xs:QName('list'), element sec:permissions { $list })\n"
                    + "),\n";

        query += "xdmp:document-get-quality($uri),\n";
        query += "doc($uri),\n";

        if (copyProperties) {
            query += "xdmp:document-properties($uri)\n";
        } else {
            query += "()\n";
        }

        Map externs = new Hashtable(1);
        externs.put(new XDBCXName("", "uri"), _uri);
        XDBCResultSequence rs = _conn.executeQuery(query, externs);
        if (!rs.hasNext())
            throw new XDBCException("unexpected empty document: " + _uri);

        metadata = new XQSyncDocumentMetadata();

        // handle node-kind, always present
        rs.next();
        String value = rs.get_String();

        if (value.equals("binary"))
            metadata.setFormat(Connection.DOC_FORMAT_BINARY);
        else if (value.equals("text"))
            metadata.setFormat(Connection.DOC_FORMAT_TEXT);
        else
            metadata.setFormat(Connection.DOC_FORMAT_XML);

        // handle collections, optional
        while (rs.getNextType() == XDBCResultSequence.XDBC_String) {
            rs.next();
            value = rs.get_String();
            metadata.addCollection(value);
        }

        // handle permissions, optional
        String role, localName;
        int capability;
        Node node, childNode;
        while (rs.getNextType() == XDBCResultSequence.XDBC_Node) {
            rs.next();

            if (!copyPermissions)
                continue;

            // permission: turn into an XDMPPermission object
            // each permission is a sec:permission node
            // children:
            // sec:capability ("read", "insert", "update")
            // and sec:role xs:unsignedLong (but we need string)
            role = null;
            capability = -1;
            node = rs.getNode().asNode();

            while ((role == null || capability == -1) && node != null
                    && node.hasChildNodes()) {
                childNode = node.getFirstChild();

                // skip empty text nodes
                if (!childNode.hasChildNodes()) {
                    node.removeChild(childNode);
                    continue;
                }

                value = childNode.getFirstChild().toString();
                localName = childNode.getLocalName();

                if (localName.equals("capability")) {
                    if (value.equals("" + XDMPPermission.UPDATE))
                        capability = XDMPPermission.UPDATE;
                    else if (value.equals("" + XDMPPermission.INSERT))
                        capability = XDMPPermission.INSERT;
                    else
                        capability = capability = XDMPPermission.READ;
                } else if (localName.equals("role-name")) {
                    role = childNode.getFirstChild().getNodeValue();
                } else {
                    // do nothing: role-id is normal, for example
                }
                node.removeChild(childNode);
            }
            if (role == null || capability == -1) {
                throw new XDBCException("malformed permission: " + node);
            }
            metadata.addPermission(new XDMPPermission(capability, role));
        }

        // handle quality, always present
        rs.next();
        metadata.setQuality(rs.get_int());

        // handle document-node, always present
        rs.next();

        // copy the entire document body: must fit in memory!
        if (metadata.isBinary()) {
            InputStream is = new BufferedInputStream(rs.getInputStream());
            contentBytes = Utilities.cat(is);
            is.close();
        } else {
            Reader r = new BufferedReader(rs.getReader());
            contentBytes = Utilities.cat(r).getBytes();
            r.close();
        }

        // handle prop:properties node, optional
        if (rs.hasNext()) {
            rs.next();
            String pString = rs.getNode().asString();
            if (pString != null)
                metadata.setProperties(pString);
        }

        rs.close();
    }

    /**
     * @param copyProperties2
     * @param copyPermissions2
     * @param path
     * @param inputPackage
     * @throws IOException
     */
    public XQSyncDocument(String _path, XQSyncPackage _pkg,
            boolean _copyPermissions, boolean _copyProperties)
            throws IOException {
        if (_path == null)
            throw new IOException("null path");

        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;

        // need the metadata first, so we know if it's binary or text or xml
        metadata = _pkg.getMetadataEntry(_path);
        if (!copyPermissions) {
            metadata.clearPermissions();
        }
        if (!copyProperties) {
            metadata.clearProperties();
        }
        // read the content: must work for bin or xml, so use bytes
        contentBytes = _pkg.getContent(_path);
    }

    /**
     * @param _path
     * @param _inputPath
     * @param _copyPermissions
     * @param _copyProperties
     * @throws IOException
     */
    public XQSyncDocument(String _path, String _inputPath,
            boolean _copyPermissions, boolean _copyProperties)
            throws IOException {
        if (_path == null)
            throw new IOException("null path");

        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;

        // need the metadata first, so we know if it's binary or text or xml
        File metaFile = new File(_inputPath, _path + METADATA_EXT);
        metadata = XQSyncDocumentMetadata.fromXML(new FileReader(metaFile));
        if (!copyPermissions) {
            metadata.clearPermissions();
        }
        if (!copyProperties) {
            metadata.clearProperties();
        }

        // read the content: must work for bin or xml, so use bytes
        File contentFile = new File(_inputPath, _path);
        contentBytes = Utilities.getBytes(contentFile);
    }

    /**
     * @param _conn
     * @param _placeKeys
     * @param readRoles
     * @return
     * @throws IOException
     * @throws XDBCException
     * @throws XDBCException
     */
    public long write(String outputPath, Connection _conn,
            Collection _readRoles, String[] _placeKeys) throws IOException,
            XDBCException {
        if (outputPath == null)
            throw new IOException("null outputPath");

        // handle deletes
        if (contentBytes == null || contentBytes.length < 1) {
            // this document has been deleted
            // no need to retry this: the connection class should handle it
            _conn.deleteDocument(outputPath);
            return 0;
        }

        // TODO optionally check to see if document is already up-to-date

        // constants
        XDMPDocInsertStream os = null;
        int repair = XDMPDocInsertStream.XDMP_ERROR_CORRECTION_NONE;
        boolean resolveEntities = false;
        String namespace = null;

        // marshal the permissions as an array
        // don't check copyProperties here:
        // if false, the constructor shouldn't have read any
        // and anyway we still want to handle any _readRoles
        if (_readRoles != null) {
            metadata.addPermissions(_readRoles);
        }
        XDMPPermission[] permissions = metadata.getPermissions();
        String[] collections = metadata.getCollections();

        // check for and retry retryable exceptions here
        long bytes = 0;
        int remainingTries = 10;
        while (remainingTries > 0) {
            remainingTries--;
            try {
                // must be able to communicate with 2.2 servers, for now
                os = _conn.getConnection().openDocInsertStream(outputPath,
                        Locale.getDefault(), resolveEntities, permissions,
                        collections, metadata.getQuality(), namespace, repair,
                        _placeKeys, metadata.getFormat(), null);

                if (metadata.isBinary()) {
                    InputStream is = new ByteArrayInputStream(contentBytes);
                    bytes = Utilities.copy(is, os);
                    is.close();
                } else {
                    Reader r = new InputStreamReader(new ByteArrayInputStream(
                            contentBytes));
                    bytes = Utilities.copy(r, os);
                    r.close();
                }
                os.flush();
                os.commit();
                os.close();
                os = null;
                // break out of the retry loop
                break;
            } catch (XDBCXQueryException e) {
                if (e.getRetryable() && remainingTries > 0) {
                    // ok, so retry it
                    // log something here, but only if we have a logger
                    if (logger != null) {
                        logger.warning("retrying on exception: "
                                + e.getLocalizedMessage());
                    }
                    if (os != null) {
                        try {
                            os.abort();
                        } catch (XDBCException dummy) {
                            // do nothing
                        }
                        try {
                            os.close();
                        } catch (IOException dummy) {
                            // do nothing
                        }
                        os = null;
                    }
                } else {
                    throw e;
                }
            }
            if (os != null) {
                os.close();
                os = null;
            }
        } // while retries

        // handle prop:properties node, optional
        // retries should be automatically handled by Connection
        // TODO would be nice to do this in the same transaction, drat it
        String properties = metadata.getProperties();
        if (copyProperties && properties != null) {
            _conn.setDocumentProperties(outputPath, properties);
        }
        return bytes;
    }

    /**
     * @param _pkg
     * @param readRoles
     * @throws IOException
     */
    public void write(String outputPath, XQSyncPackage _pkg,
            Collection _readRoles) throws IOException {
        if (outputPath == null)
            throw new IOException("null outputPath");

        synchronized (_pkg) {
            _pkg.write(outputPath, contentBytes, metadata);
        }
    }

    /**
     * @return
     */
    public boolean isBinary() {
        return metadata.isBinary();
    }

    /**
     * @param _path
     * @return
     */
    static String getMetadataPath(String _path) {
        return _path + METADATA_EXT;
    }

    /**
     * @param readRoles
     * @throws IOException
     */
    public void write(String outputPath, Collection readRoles)
            throws IOException {
        if (outputPath == null)
            throw new IOException("null outputPath");

        File parent = new File(outputPath).getParentFile();
        if (!parent.exists())
            parent.mkdirs();

        FileOutputStream fos = new FileOutputStream(outputPath);
        fos.write(contentBytes);
        fos.close();

        String metadataPath = getMetadataPath(outputPath);
        FileOutputStream mfos = new FileOutputStream(metadataPath);
        mfos.write(metadata.toXML().getBytes());
        mfos.close();
    }

    /**
     * @param _logger
     */
    public void setLogger(SimpleLogger _logger) {
        logger = _logger;
    }

}
