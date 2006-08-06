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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collection;

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.DocumentRepairLevel;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.exceptions.XccException;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XSInteger;
import com.marklogic.xcc.types.XdmElement;

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

    private int connMajorVersion = 3;

    /**
     * @param _session
     * @param _uri
     * @param _copyPermissions
     * @param _copyProperties
     * @throws XccException
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public XQSyncDocument(com.marklogic.ps.Session _session, String _uri,
            boolean _copyPermissions, boolean _copyProperties)
            throws XccException, IOException,
            ParserConfigurationException, SAXException {
        if (_uri == null)
            throw new UnimplementedFeatureException("null uri");

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
                + "if (xdmp:exists(doc($uri))) then ()\n"
                + "else error(text{'document with uri', xdmp:describe($uri), 'does not exist'}),"
                + "node-kind(doc($uri)/node()),\n"
                + "xdmp:document-get-collections($uri),\n";

        // use node for permissions, since we walk the tree
        if (copyPermissions)
            query += "let $list := xdmp:document-get-permissions($uri)\n"
                    + "let $query := concat(\n"
                    + (connMajorVersion > 2 ? "' import module ''http://marklogic.com/xdmp/security'' at ''/MarkLogic/security.xqy''',\n"
                            : "' import module ''http://marklogic.com/xdmp/security'' at ''/security.xqy''',\n")
                    + "' define variable $list as element(sec:permissions) external',\n"
                    + "' for $p in $list/sec:permission',\n"
                    + "' return element sec:permission {',\n"
                    + "'  $p/node(), sec:get-role-names($p/sec:role-id)',\n"
                    + "' }'\n"
                    + ")\n"
                    + "return if (empty($list)) then () else\n"
                    // TODO deprecated use of eval-in (3.1)
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

        Request req = _session.newAdhocQuery(query);
        req.setNewStringVariable("uri", _uri);
        ResultSequence rs = _session.submitRequest(req);

        if (!rs.hasNext()) {
            throw new UnimplementedFeatureException(
                    "unexpected empty document: " + _uri);
        }

        metadata = new XQSyncDocumentMetadata();
        ResultItem[] items = rs.toResultItemArray();

        // handle node-kind, always present
        metadata.setFormat(items[0].asString());

        int index = 1;

        // handle collections, optional
        while (index < items.length
                && items[index].getItemType() == ValueType.XS_STRING) {
            metadata.addCollection(items[index].asString());
            index++;
        }

        // handle permissions, optional
        Element permissionElement;
        NodeList capabilities, roles;
        while (index < items.length
                && items[index].getItemType() == ValueType.ELEMENT) {
            if (!copyPermissions) {
                index++;
                continue;
            }

            // permission: turn into a ContentPermission object
            // each permission is a sec:permission element
            // children:
            // sec:capability ("read", "insert", "update")
            // and sec:role xs:unsignedLong (but we need string)
            permissionElement = ((XdmElement) items[index].getItem())
                    .asW3cElement();

            capabilities = permissionElement
                    .getElementsByTagName("capability");
            roles = permissionElement.getElementsByTagName("role-name");
            if (roles.getLength() != 1 || capabilities.getLength() != 1) {
                throw new UnimplementedFeatureException(
                        "bad input permission: " + permissionElement);
            }
            metadata.addPermission(capabilities.item(0).getNodeValue(),
                    roles.item(0).getNodeValue());
            index++;
        }

        // handle quality, always present
        metadata.setQuality((XSInteger) items[index].getItem());
        index++;

        // handle document-node, always present
        if (metadata.isBinary()) {
            setContentBytes(items[index].asInputStream());
        } else {
            setContentBytes(items[index].asReader());
        }
        index++;

        // handle prop:properties node, optional
        if (index < items.length) {
            String pString = items[index].asString();
            if (pString != null) {
                metadata.setProperties(pString);
            }
        }

        rs.close();
    }

    /**
     * @param reader
     * @throws IOException
     */
    private void setContentBytes(Reader reader) throws IOException {
        contentBytes = Utilities.cat(reader).getBytes();
    }

    /**
     * @param stream
     * @throws IOException
     */
    private void setContentBytes(InputStream stream) throws IOException {
        contentBytes = Utilities.cat(stream);
    }

    /**
     * @param _path
     * @param _copyPermissions
     * @param _copyProperties
     * @param _inputPackage
     * @throws IOException
     */
    public XQSyncDocument(XQSyncPackage _pkg, String _path,
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
     * @param _file
     * @param _copyPermissions
     * @param _copyProperties
     * @throws IOException
     */
    public XQSyncDocument(File _file, boolean _copyPermissions,
            boolean _copyProperties) throws IOException {
        // read the content: must work for bin or xml, so use bytes
        contentBytes = Utilities.getBytes(_file);

        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;

        File metaFile = getMetadataFile(_file);
        metadata = XQSyncDocumentMetadata
                .fromXML(new FileReader(metaFile));
        if (!copyPermissions) {
            metadata.clearPermissions();
        }
        if (!copyProperties) {
            metadata.clearProperties();
        }
    }

    static File getMetadataFile(File contentFile) throws IOException {
        return new File(getMetadataPath(contentFile));
    }

    /**
     * @param _outputUri
     * @param _session
     * @param _placeKeys
     * @param _readRoles
     * @return
     * @throws IOException
     * @throws XccException
     */
    public long write(String _outputUri,
            com.marklogic.ps.Session _session,
            Collection<ContentPermission> _readRoles, String[] _placeKeys)
            throws IOException, XccException {
        if (_outputUri == null) {
            throw new IOException("null outputPath");
        }

        logger.fine(_outputUri);

        // handle deletes
        if (contentBytes == null || contentBytes.length < 1) {
            // this document has been deleted
            _session.deleteDocument(_outputUri);
            return 0;
        }

        // TODO optionally check to see if document is already up-to-date

        // constants
        DocumentRepairLevel repair = DocumentRepairLevel.NONE;
        boolean resolveEntities = false;
        String namespace = null;

        // marshal the permissions as an array
        // don't check copyProperties here:
        // if false, the constructor shouldn't have read any
        // and anyway we still want to handle any _readRoles
        metadata.addPermissions(_readRoles);
        ContentPermission[] permissions = metadata.getPermissions();
        String[] collections = metadata.getCollections();

        ContentCreateOptions options = null;
        if (metadata.isBinary()) {
            options = ContentCreateOptions.newBinaryInstance();
        } else if (metadata.isText()) {
            options = ContentCreateOptions.newTextInstance();
        } else {
            options = ContentCreateOptions.newXmlInstance();
        }

        options.setResolveEntities(resolveEntities);
        options.setPermissions(permissions);
        options.setCollections(collections);
        options.setQuality(metadata.getQuality());
        options.setNamespace(namespace);
        options.setRepairLevel(repair);
        options.setPlaceKeys(_session.forestNamesToIds(_placeKeys));

        Content content = ContentFactory.newContent(_outputUri,
                contentBytes, options);
        _session.insertContent(content);
        _session.commit();

        // handle prop:properties node, optional
        // TODO would be nice to do this in the same transaction, drat it
        String properties = metadata.getProperties();
        if (copyProperties && properties != null) {
            _session.setDocumentProperties(_outputUri, properties);
        }
        return contentBytes.length;
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

        _pkg.write(outputPath, contentBytes, metadata);
        // the caller has to flush() the pkg
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
     * @param _path
     * @return
     * @throws IOException
     */
    static String getMetadataPath(File _file) throws IOException {
        return _file.getCanonicalPath() + METADATA_EXT;
    }

    /**
     * @param outputFile
     * @throws IOException
     */
    public void write(File outputFile) throws IOException {
        File parent = outputFile.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }

        if (!parent.isDirectory()) {
            throw new IOException("parent is not a directory: "
                    + parent.getCanonicalPath());
        }

        if (!parent.canWrite()) {
            throw new IOException("cannot write to parent directory: "
                    + parent.getCanonicalPath());
        }

        FileOutputStream fos = new FileOutputStream(outputFile);
        fos.write(contentBytes);
        fos.flush();
        fos.close();

        String metadataPath = getMetadataPath(outputFile);
        FileOutputStream mfos = new FileOutputStream(metadataPath);
        mfos.write(metadata.toXML().getBytes());
        mfos.flush();
        mfos.close();
    }

    /**
     * @param _logger
     */
    public void setLogger(SimpleLogger _logger) {
        logger = _logger;
    }

}
