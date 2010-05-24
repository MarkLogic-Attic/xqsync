/**
 * Copyright (c) 2008-2010 Mark Logic Corporation. All rights reserved.
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

import java.io.IOException;
import java.math.BigInteger;

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.marklogic.ps.Session;
import com.marklogic.ps.Utilities;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.exceptions.XccException;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XSInteger;
import com.marklogic.xcc.types.XdmElement;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class SessionReader extends AbstractReader {

    protected static String query;

    private BigInteger timestamp;

    private String inputModule;

    protected boolean copyPermissions;

    protected boolean copyProperties;

    protected boolean copyCollections;

    protected boolean copyQuality;

    protected int size = 1;

    /**
     * @param _configuration
     * @throws SyncException
     */
    public SessionReader(Configuration _configuration)
            throws SyncException {
        // superclass takes care of some configuration
        super(_configuration);

        copyPermissions = configuration.isCopyPermissions();
        copyProperties = configuration.isCopyProperties();
        copyCollections = configuration.isCopyCollections();
        copyQuality = configuration.isCopyQuality();

        timestamp = configuration.getTimestamp();
        inputModule = configuration.getInputModule();

        size = configuration.getInputBatchSize();

        try {
            if (null == query) {
                initQuery();
            }
        } catch (RequestException e) {
            throw new SyncException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.ReaderInterface#read(java.lang.String,
     * com.marklogic.ps.xqsync.DocumentInterface)
     */
    public void read(String[] _uris, DocumentInterface _document)
            throws SyncException {
        if (null == _uris) {
            throw new SyncException("null uris");
        }
        if (_uris.length != size) {
            throw new SyncException("bad uris, " + _uris.length + " != "
                    + size);
        }
        if (null == _document) {
            throw new SyncException("null document");
        }

        ResultSequence rs = null;
        Session session = null;
        int urisIndex = 0;
        try {
            // in case the server or network is unreliable, try three times
            int retries = 3;
            while (retries > 0) {
                urisIndex = 0;
                try {
                    // retry around all session-related objects
                    session = configuration.newInputSession();
                    if (null == session) {
                        throw new FatalException("null input session");
                    }
                    RequestOptions opts = session
                            .getDefaultRequestOptions();
                    if (null != timestamp) {
                        opts.setEffectivePointInTime(timestamp);
                    }
                    opts.setResultBufferSize(configuration
                            .inputResultBufferSize());
                    Request req = session.newAdhocQuery(query, opts);
                    for (int i = 0; i < _uris.length; i++) {
                        req.setNewStringVariable("URI-" + i,
                                null != _uris[i] ? _uris[i] : "");
                    }
                    req.setNewStringVariable("MODULE-URI",
                            (null == inputModule) ? "" : inputModule);

                    rs = session.submitRequest(req);
                    // success!
                    break;
                } catch (XQueryException e) {
                    // probably an XQuery syntax error - do not retry
                    // we want to know what the uri and query were.
                    String uris = Utilities.join(_uris, "; ");
                    logger.severe("error in document, uri = " + uris);
                    logger.severe("error in query: " + query);
                    throw new SyncException(uris, e);
                } catch (XccException e) {
                    retries--;
                    // we want to know which document it was
                    String uris = Utilities.join(_uris, "; ");
                    if (retries < 1) {
                        logger.severe("retries exhausted for " + uris);
                        throw new SyncException(uris, e);
                    }
                    logger.warning("error reading document: will retry ("
                            + retries + "): " + uris + " due to "
                            + e.getMessage());
                    Thread.yield();
                    // if the session "went bad", we'll want a new one
                    if (null != session && !session.isClosed()) {
                        session.close();
                    }
                    session = configuration.newInputSession();
                }
            }

            if (!rs.hasNext()) {
                throw new SyncException("unexpected empty document: "
                        + _uris[urisIndex]);
            }

            ResultItem[] items = rs.toResultItemArray();

            int resultIndex = 0;

            while (resultIndex < items.length) {
                logger.fine("resultIndex " + resultIndex + "/"
                        + items.length);
                if (null == _uris[urisIndex]) {
                    logger.fine("uri at " + urisIndex + " is null");
                    break;
                }
                resultIndex = readDocument(_document, items, urisIndex,
                        resultIndex);
                urisIndex++;
            }

            // pre-empt the finally block
            cleanup(session, rs);

        } finally {
            cleanup(session, rs);
        }
    }

    private int readDocument(DocumentInterface _document,
            ResultItem[] _items, int _urisIndex, int _resultIndex)
            throws SyncException {
        MetadataInterface metadata = _document.newMetadata();

        // handle node-kind, always present
        String format = _items[_resultIndex].asString();
        logger.finer("format = " + format);
        metadata.setFormat(format);
        _resultIndex++;

        // handle collections, may not be present
        while (_resultIndex < _items.length
                && _items[_resultIndex].getItemType() == ValueType.XS_STRING) {
            if (!copyCollections) {
                _resultIndex++;
                continue;
            }
            metadata.addCollection(_items[_resultIndex].asString());
            _resultIndex++;
        }

        // handle permissions, may not be present
        while (_resultIndex < _items.length
                && ValueType.ELEMENT == _items[_resultIndex]
                        .getItemType()) {
            if (!copyPermissions) {
                _resultIndex++;
                continue;
            }
            readPermission((XdmElement) _items[_resultIndex].getItem(),
                    metadata);
            _resultIndex++;
        }

        // handle quality, always present even if not requested (barrier)
        metadata.setQuality((XSInteger) _items[_resultIndex].getItem());
        _resultIndex++;

        // handle document-node, always present
        if (metadata.isBinary()) {
            _document.setContent(_urisIndex, _items[_resultIndex]
                    .asInputStream());
        } else {
            _document.setContent(_urisIndex, _items[_resultIndex]
                    .asReader());
        }
        _resultIndex++;

        // handle prop:properties node, optional
        // if not present, there will be a 0 as a marker
        if (copyProperties
                && ValueType.ELEMENT == _items[_resultIndex]
                        .getItemType()) {
            String pString = _items[_resultIndex].asString();
            if (pString != null) {
                metadata.setProperties(pString);
            }
            _resultIndex++;
        }

        // verify marker
        if (ValueType.XS_INTEGER != _items[_resultIndex].getItemType()) {
            throw new SyncException("unexpected "
                    + _items[_resultIndex].getItemType() + " "
                    + _items[_resultIndex].asString() + ", expected "
                    + ValueType.XS_INTEGER + " 0");
        }
        _resultIndex++;

        _document.setMetadata(_urisIndex, metadata);
        return _resultIndex;
    }

    protected void cleanup(Session session, ResultSequence rs) {
        if (null != rs && !rs.isClosed()) {
            rs.close();
        }
        if (null != session && !session.isClosed()) {
            session.close();
        }
    }

    /**
     * @param _permissionElement
     * @param _metadata
     * @throws SyncException
     */
    private void readPermission(XdmElement _permissionElement,
            MetadataInterface _metadata) throws SyncException {
        // permission: turn into a ContentPermission object
        // each permission is a sec:permission element.
        // children:
        // sec:capability ("read", "insert", "update")
        // and sec:role xs:unsignedLong (but we need string)
        logger.fine("permissionElement = "
                + _permissionElement.asString());
        try {
            Element permissionW3cElement = _permissionElement
                    .asW3cElement();
            logger.fine("permissionElement = "
                    + permissionW3cElement.toString());

            NodeList capabilities = permissionW3cElement
                    .getElementsByTagName("sec:capability");
            NodeList roles = permissionW3cElement
                    .getElementsByTagName("sec:role-name");
            Node role;
            Node capability;
            if (0 < roles.getLength() && 0 < capabilities.getLength()) {
                role = roles.item(0);
                capability = capabilities.item(0);
                _metadata.addPermission(capability.getTextContent(), role
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
        } catch (ParserConfigurationException e) {
            throw new SyncException(e);
        } catch (IOException e) {
            throw new SyncException(e);
        } catch (SAXException e) {
            throw new SyncException(e);
        }
    }

    /**
     * @throws RequestException
     * 
     */
    private synchronized void initQuery() throws RequestException {
        if (null != query) {
            return;
        }

        if (null != inputModule) {
            logger.info("using " + Configuration.INPUT_MODULE_URI_KEY
                    + "=" + inputModule);
        }

        // easy to distinguish the result-sets: metadata, data, properties
        // first is node-kind as string
        // then collection strings (if requested and present)
        // then permission nodes (if requested and present)
        // then quality integer, always present (default 0)
        // then the document-node
        // then property node (if requested and present)
        // one wrinkle: get-permissions() returns sec:permission/sec:role-id,
        // but our callers need the role-name! this gets ugly...
        // must wrap the list of permissions so we can pass it...
        //
        // normally I would put this code in a module,
        // but I want this program to be self-contained
        query = Session.XQUERY_VERSION_0_9_ML;

        // prolog - some variables are per-input
        query += "define variable $MODULE-URI as xs:string external\n";

        // we should not normally have to guard against multiple docs per URI
        String predicate = configuration
                .isRepairMultipleDocumentsPerUri() ? "[1]" : "";

        for (int i = 0; i < size; i++) {
            // TODO - support for naked properties?
            query += "define variable $URI-" + i
                    + " as xs:string external\n"
                    + "define variable $DOC-" + i
                    + " as document-node() {\n" + "  if ($URI-" + i
                    + " eq '') then document { () }\n"
                    + "  else if ($MODULE-URI) then xdmp:invoke(\n"
                    + "    $MODULE-URI, (xs:QName('URI'), $URI-" + i
                    + "))" + predicate + "\n"
                    + "  else doc($URI-"
                    + i
                    + ")"
                    + predicate
                    + "\n"
                    + "}\n"
                    // a document may contain multiple root nodes
                    // we will prefer the element(), if present
                    + "define variable $ROOT-"
                    + i
                    + " as node()? {\n"
                    // no need to check for document-node, attribute, namespace
                    + " (\n" + "  $DOC-" + i + "/element(), $DOC-" + i
                    + "/binary(), $DOC-" + i + "/comment(),\n"
                    + "  $DOC-" + i + "/processing-instruction(), $DOC-"
                    + i + "/text()\n" + " )[1] }\n";
        }

        // body, once per input
        for (int i = 0; i < size; i++) {
            query += (0 == i ? "\n" : ",\n");

            // NB - empty document is equivalent to an empty text node
            query += "if ($ROOT-" + i + ") then node-kind($ROOT-" + i
                    + ") else 'text',\n";

            if (copyCollections) {
                query += "if ($URI-" + i + " eq '') then ()\n"
                        + "else xdmp:document-get-collections($URI-" + i
                        + "),\n";
            }

            // use node for permissions, since we walk the tree
            if (copyPermissions) {
                query += "let $list := \n"
                        + "  if ($URI-"
                        + i
                        + " eq '') then ()\n"
                        + "else xdmp:document-get-permissions($URI-"
                        + i
                        + ")\n"
                        + "let $query := concat(\n"
                        + "' import module ''http://marklogic.com/xdmp/security''"
                        + " at ''/MarkLogic/security.xqy''',\n"
                        + "' define variable $LIST as element(sec:permissions) external',\n"
                        + "' for $p in $LIST/sec:permission',\n"
                        + "' return element sec:permission {',\n"
                        + "'  $p/@*, $p/node(), sec:get-role-names($p/sec:role-id)',\n"
                        + "' }'\n"
                        + ")\n"
                        // TODO replace eval-in with eval, after 3.1 EOL
                        // (2009-06-01)
                        + "where exists($list)\n"
                        + "return xdmp:eval-in(\n"
                        + "  $query, xdmp:security-database(),\n"
                        + "  (xs:QName('LIST'), element sec:permissions { $list })\n"
                        + "),\n";
            }

            // quality acts as a marker between permissions and the node
            if (copyQuality) {
                query += "if ($URI-" + i + " eq '') then 0\n"
                        + "else xdmp:document-get-quality($URI-" + i
                        + "),\n";
            } else {
                query += "0,";
            }

            query += "$DOC-" + i + ",\n";

            if (copyProperties) {
                query += "if ($URI-" + i + " eq '') then ()\n"
                        + "else xdmp:document-properties($URI-" + i
                        + "),\n";
            } else {
                query += "(),\n";
            }

            // end-of-record marker
            query += "0\n";
        }
    }
}
