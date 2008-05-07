/*
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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.util.Collection;

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
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
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.exceptions.XccException;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XSInteger;
import com.marklogic.xcc.types.XdmElement;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class XQSyncDocument {

    /**
     * 
     */
    private static final String UTF_8 = "UTF-8";

    /**
     * 
     */
    private static final String ENCODING = UTF_8;

    public static final String METADATA_EXT = ".metadata";

    public static final String METADATA_REGEX = "^.+\\"
            + XQSyncDocument.METADATA_EXT + "$";

    private byte[] contentBytes;

    private XQSyncDocumentMetadata metadata;

    private boolean copyPermissions = true;

    private boolean copyProperties = true;

    private static SimpleLogger logger = null;

    private int connMajorVersion = 3;

    private String outputPathPrefix;

    private String inputUri;

    private boolean repairInputXml = false;

    private BigInteger timestamp = null;

    /**
     * @param _session
     * @param _uri
     * @param _copyPermissions
     * @param _copyProperties
     * @param _repairInputXml
     * @param _timestamp
     * 
     * @throws IOException
     * @throws RequestException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public XQSyncDocument(com.marklogic.ps.Session _session, String _uri,
            boolean _copyPermissions, boolean _copyProperties,
            boolean _repairInputXml, BigInteger _timestamp)
            throws IOException, RequestException,
            ParserConfigurationException, SAXException {
        if (_uri == null) {
            throw new UnimplementedFeatureException("null uri");
        }

        inputUri = _uri;
        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;
        repairInputXml = _repairInputXml;
        timestamp = _timestamp;

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
        String query = "define variable $URI as xs:string external\n"
                + "define variable $DOC as document-node() { doc($URI) }\n"
                // cf bug 3575 - document allows multiple roots
                // we will prefer the element(), if present
                + "define variable $ROOT as node()+ {\n"
                + " ($DOC/element(), $DOC/binary(), $DOC/comment(),\n"
                + "  $DOC/processing-instruction(), $DOC/text() )[1] }\n"
                + "node-kind($ROOT),\n"
                + "xdmp:document-get-collections($URI),\n";

        // use node for permissions, since we walk the tree
        if (copyPermissions) {
            query += "let $list := xdmp:document-get-permissions($URI)\n"
                    + "let $query := concat(\n"
                    + (connMajorVersion > 2 ? "' import module ''http://marklogic.com/xdmp/security'' at ''/MarkLogic/security.xqy''',\n"
                            : "' import module ''http://marklogic.com/xdmp/security'' at ''/security.xqy''',\n")
                    + "' define variable $LIST as element(sec:permissions) external',\n"
                    + "' for $p in $LIST/sec:permission',\n"
                    + "' return element sec:permission {',\n"
                    + "'  $p/@*, $p/node(), sec:get-role-names($p/sec:role-id)',\n"
                    + "' }'\n"
                    + ")\n"
                    // TODO deprecated use of eval-in (3.1)
                    + "where exists($list)\n"
                    + "return xdmp:eval-in(\n"
                    + "  $query, xdmp:security-database(),\n"
                    + "  (xs:QName('LIST'), element sec:permissions { $list })\n"
                    + "),\n";
        }

        query += "xdmp:document-get-quality($URI),\n";
        query += "$DOC,\n";

        if (copyProperties) {
            query += "xdmp:document-properties($URI)\n";
        } else {
            query += "()\n";
        }

        Request req = null;
        if (null != timestamp) {
            RequestOptions opts = _session.getDefaultRequestOptions();
            opts.setEffectivePointInTime(timestamp);
            req = _session.newAdhocQuery(query, opts);
        } else {
            req = _session.newAdhocQuery(query);
        }
        req.setNewStringVariable("URI", _uri);
        ResultSequence rs = null;
        int retries = 3;
        // in case the server is unreliable, we try three times
        while (retries > 0) {
            try {
                rs = _session.submitRequest(req);
                // success!
                break;
            } catch (XQueryException e) {
                // probably an XQuery syntax error - do not retry
                // we want to know what the uri and query were.
                logger.severe("error in document, uri = " + _uri);
                logger.severe("error in query: " + query);
                throw new RequestException(_uri, req, e);
            } catch (XccException e) {
                retries--;
                // we want to know which document it was
                if (retries < 1) {
                    logger.severe("retries exhausted for " + _uri);
                    throw new RequestException(_uri, req, e);
                }
                logger.logException(
                        "error reading document: will retry (" + retries
                                + "): " + _uri, e);
                Thread.yield();
            }
        }

        if (!rs.hasNext()) {
            throw new UnimplementedFeatureException(
                    "unexpected empty document: " + _uri);
        }

        metadata = new XQSyncDocumentMetadata();
        ResultItem[] items = rs.toResultItemArray();

        // handle node-kind, always present
        String format = items[0].asString();
        logger.finer("format = " + format);
        metadata.setFormat(format);

        int index = 1;

        // handle collections, optional
        while (index < items.length
                && items[index].getItemType() == ValueType.XS_STRING) {
            metadata.addCollection(items[index].asString());
            index++;
        }

        // handle permissions, optional
        Element permissionW3cElement;
        XdmElement permissionElement;
        NodeList capabilities, roles;
        Node capability, role;
        while (index < items.length
                && items[index].getItemType() == ValueType.ELEMENT) {
            logger.fine("copyPermissions = " + copyPermissions);
            if (!copyPermissions) {
                index++;
                continue;
            }

            // permission: turn into a ContentPermission object
            // each permission is a sec:permission element.
            // children:
            // sec:capability ("read", "insert", "update")
            // and sec:role xs:unsignedLong (but we need string)
            permissionElement = (XdmElement) items[index].getItem();
            logger.fine("permissionElement = "
                    + permissionElement.asString());
            permissionW3cElement = permissionElement.asW3cElement();
            logger.fine("permissionElement = "
                    + permissionW3cElement.toString());

            capabilities = permissionW3cElement
                    .getElementsByTagName("sec:capability");
            roles = permissionW3cElement
                    .getElementsByTagName("sec:role-name");
            if (0 < roles.getLength() && 0 < capabilities.getLength()) {
                role = roles.item(0);
                capability = capabilities.item(0);
                metadata.addPermission(capability.getTextContent(), role
                        .getTextContent());
                if (roles.getLength() > 1) {
                    logger.warning("input permission: "
                            + permissionW3cElement + ": "
                            + roles.getLength() + " roles, using only 1");
                }
                if (capabilities.getLength() > 1) {
                    logger.warning("input permission: "
                            + permissionW3cElement + ": "
                            + capabilities.getLength()
                            + " capabilities, using only 1");
                }
            } else {
                // warn and skip
                if (roles.getLength() < 1) {
                    logger.warning("skipping input permission: "
                            + permissionW3cElement + ": no roles");
                }
                if (capabilities.getLength() < 1) {
                    logger.warning("skipping input permission: "
                            + permissionW3cElement + ": no capabilities");
                }
            }
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

    private void setContentBytes(byte[] _contentBytes) {
        contentBytes = _contentBytes;
    }

    /**
     * @param reader
     * @throws IOException
     */
    private void setContentBytes(Reader reader) throws IOException {
        setContentBytes(Utilities.cat(reader).getBytes());
    }

    /**
     * @param stream
     * @throws IOException
     */
    private void setContentBytes(InputStream stream) throws IOException {
        setContentBytes(Utilities.cat(stream));
    }

    /**
     * @param _pkg
     * @param _path
     * @param _copyPermissions
     * @param _copyProperties
     * @param _repairInputXml
     * @param _allowEmptyMetadata
     * @throws IOException
     */
    public XQSyncDocument(InputPackage _pkg, String _path,
            boolean _copyPermissions, boolean _copyProperties,
            boolean _repairInputXml, boolean _allowEmptyMetadata)
            throws IOException {
        if (_path == null) {
            throw new IOException("null path");
        }

        inputUri = _path;
        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;
        repairInputXml = _repairInputXml;

        // need the metadata first, so we know if it's binary or text or xml
        try {
            metadata = _pkg.getMetadataEntry(_path);
            if (!copyPermissions) {
                metadata.clearPermissions();
            }
            if (!copyProperties) {
                metadata.clearProperties();
            }
        } catch (IOException e) {
            // optionally allow empty metadata
            if (_allowEmptyMetadata) {
                metadata = new XQSyncDocumentMetadata();
            } else {
                throw e;
            }
        }
        // read the content: must work for bin or xml, so use bytes
        setContentBytes(_pkg.getContent(_path));
    }

    /**
     * @param _file
     * @param _copyPermissions
     * @param _copyProperties
     * @param _repairInputXml
     * @param _allowEmptyMetadata
     * @throws IOException
     */
    public XQSyncDocument(File _file, boolean _copyPermissions,
            boolean _copyProperties, boolean _repairInputXml,
            boolean _allowEmptyMetadata) throws IOException {
        inputUri = _file.getCanonicalPath();
        copyPermissions = _copyPermissions;
        copyProperties = _copyProperties;
        repairInputXml = _repairInputXml;

        try {
            metadata = XQSyncDocumentMetadata.fromXML(new FileReader(
                    getMetadataFile(_file)));
            if (!copyPermissions) {
                metadata.clearPermissions();
            }
            if (!copyProperties) {
                metadata.clearProperties();
            }
        } catch (IOException e) {
            // optionally allow empty metadata
            if (_allowEmptyMetadata) {
                metadata = new XQSyncDocumentMetadata();
            } else {
                throw e;
            }
        }

        // read the content: must work for bin or xml, so use bytes
        setContentBytes(Utilities.cat(_file));
    }

    /**
     * @param _uri
     * @param _isEncoded
     * @throws UnsupportedEncodingException
     * 
     * for unit-tests only
     * 
     */
    public XQSyncDocument(String _uri, boolean _isEncoded)
            throws UnsupportedEncodingException {
        inputUri = _isEncoded ? URLDecoder.decode(_uri, ENCODING) : _uri;
    }

    static File getMetadataFile(File contentFile) throws IOException {
        return new File(getMetadataPath(contentFile));
    }

    /**
     * @param _session
     * @param _placeKeys
     * @param _readRoles
     * @param _skipExisting
     * @return
     * @throws XccException
     */
    public long write(com.marklogic.ps.Session _session,
            Collection<ContentPermission> _readRoles,
            String[] _placeKeys, boolean _skipExisting)
            throws XccException {
        String outputUri = composeOutputUri(false);

        logger.finest("placeKeys = " + Utilities.join(_placeKeys, ","));

        int retries = 3;
        // in case the server is unreliable, we try three times
        while (retries > 0) {
            try {
                // handle deletes
                if (null == contentBytes || contentBytes.length < 1) {
                    // this document has been deleted
                    _session.deleteDocument(outputUri);
                    return 0;
                }

                // optionally, check to see if document is already up-to-date
                if (_skipExisting && _session.existsDocument(outputUri)) {
                    logger.fine("skipping existing document: "
                            + outputUri);
                    return 0;
                }

                // constants
                boolean resolveEntities = false;
                String namespace = null;

                DocumentRepairLevel repair = (!repairInputXml) ? DocumentRepairLevel.NONE
                        : DocumentRepairLevel.FULL;
                logger.fine("repair = " + repairInputXml + ", " + repair);

                // marshal the permissions as an array
                // don't check copyProperties here:
                // if false, the constructor shouldn't have read any
                // and anyway we still want to handle any _readRoles
                metadata.addPermissions(_readRoles);
                ContentPermission[] permissions = metadata
                        .getPermissions();
                String[] collections = metadata.getCollections();
                logger.fine("collections = "
                        + Utilities.join(collections, " "));

                ContentCreateOptions options = null;
                if (metadata.isBinary()) {
                    logger.fine(inputUri + " is binary");
                    options = ContentCreateOptions.newBinaryInstance();
                } else if (metadata.isText()) {
                    logger.fine(inputUri + " is text");
                    options = ContentCreateOptions.newTextInstance();
                } else {
                    logger.fine(inputUri + " is xml");
                    options = ContentCreateOptions.newXmlInstance();
                }

                options.setResolveEntities(resolveEntities);
                options.setPermissions(permissions);
                options.setCollections(collections);
                options.setQuality(metadata.getQuality());
                options.setNamespace(namespace);
                options.setRepairLevel(repair);
                options.setPlaceKeys(_session
                        .forestNamesToIds(_placeKeys));

                Content content = ContentFactory.newContent(outputUri,
                        contentBytes, options);

                _session.insertContent(content);
                _session.commit();

                // handle prop:properties node, optional
                // TODO would be nice to do this in the same transaction, drat
                // it
                String properties = metadata.getProperties();
                if (copyProperties && properties != null) {
                    _session.setDocumentProperties(outputUri, properties);
                }
                // success - will not loop
                break;
            } catch (XQueryException e) {
                throw e;
            } catch (XccException e) {
                retries--;
                // we want to know which document it was
                if (retries < 1) {
                    logger.severe("retries exhausted for " + outputUri);
                    throw e;
                }
                logger.logException(
                        "error writing document: will retry (" + retries
                                + "): " + outputUri, e);
                Thread.yield();
            }
        }
        return contentBytes.length;
    }

    /**
     * @param _pkg
     * @param readRoles
     * @throws IOException
     */
    public void write(OutputPackage _pkg,
            Collection<ContentPermission> _readRoles) throws IOException {
        String outputUri = composeOutputUri(true);
        _pkg.write(outputUri, contentBytes, metadata);
        // the caller has to flush() the package
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
     * @throws IOException
     */
    public void write() throws IOException {
        String outputUri = composeOutputUri(true);
        File outputFile = new File(outputUri);
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
    public static void setLogger(SimpleLogger _logger) {
        logger = _logger;
        logger.fine("new logger");
    }

    /**
     * @param _path
     */
    public void setOutputUriPrefix(String _path) {
        // build remote URI from outputPath and uri
        // path may be empty: if not, it should end with separator
        outputPathPrefix = _path;
    }

    public String composeOutputUri(boolean isEscaped) {
        if (null != outputPathPrefix && !outputPathPrefix.equals("")
                && !outputPathPrefix.endsWith("/")) {
            outputPathPrefix += "/";
        }

        String outputUri = (null == outputPathPrefix ? ""
                : outputPathPrefix)
                + inputUri;
        // TODO optionally escape outputUri
        // note that some constructors will need to unescape the inputUri
        if (isEscaped) {
            // NTFS: The period (.) cannot be the first character
            // NTFS: Illegal Characters: / \ : * ? " < > |
            // TODO note that this is a dummy at present.
            // it's unclear when and what needs to be escaped.
            // outputUri = URLEncoder.encode(outputUri, ENCODING);
        }
        logger.finer("copying " + inputUri + " to " + outputUri);
        return outputUri;
    }

    /**
     * @return
     */
    public String getOutputUri() {
        return composeOutputUri(false);
    }

    /**
     * @param _collections
     */
    public void addOutputCollections(String[] _collections) {
        metadata.addCollections(_collections);
    }

    public void setRepairInputXml(boolean repairInputXml) {
        this.repairInputXml = repairInputXml;
    }

    public byte[] getContentBytes() {
        return contentBytes;
    }

    /**
     * @return
     */
    public XQSyncDocumentMetadata getMetadata() {
        return metadata;
    }

    /**
     * @return
     */
    public long getContentBytesLength() {
        return contentBytes.length;
    }

}
