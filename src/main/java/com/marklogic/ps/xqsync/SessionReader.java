/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c) 2008-2022 MarkLogic Corporation. All rights reserved.
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
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class SessionReader extends AbstractReader {

    protected static volatile String query = null;
    private final BigInteger timestamp;
    private final String inputModule;
    protected final boolean copyPermissions;
    protected final boolean copyProperties;
    protected final boolean copyCollections;
    protected final boolean copyQuality;
    protected final boolean isIndented;
    protected int size = 1;

    /**
     * @param configuration
     * @throws SyncException
     */
    public SessionReader(Configuration configuration) throws SyncException {
        // superclass takes care of some configuration
        super(configuration);

        copyPermissions = configuration.isCopyPermissions();
        copyProperties = configuration.isCopyProperties();
        copyCollections = configuration.isCopyCollections();
        copyQuality = configuration.isCopyQuality();
	      isIndented = configuration.getInputIndented();
        timestamp = configuration.getTimestamp();
        inputModule = configuration.getInputModule();
        size = configuration.getInputBatchSize();

        if (null == query) {
            initQuery();
            logger.fine("reader query = \n" + query);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.ReaderInterface#read(java.lang.String,
     * com.marklogic.ps.xqsync.DocumentInterface)
     */
    public void read(String[] uris, DocumentInterface document) throws SyncException {
        if (null == uris) {
            throw new SyncException("null uris");
        }
        if (uris.length != size) {
            throw new SyncException("bad uris, " + uris.length + " != " + size);
        }
        if (null == document) {
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
                    RequestOptions opts = session.getDefaultRequestOptions();
                    if (null != timestamp) {
                        opts.setEffectivePointInTime(timestamp);
                    }
                    opts.setResultBufferSize(configuration.inputResultBufferSize());
                    Request req = session.newAdhocQuery(query, opts);
                    for (int i = 0; i < uris.length; i++) {
                        req.setNewStringVariable("URI-" + i, null != uris[i] ? uris[i] : "");
                    }
                    req.setNewStringVariable("MODULE-URI", (null == inputModule) ? "" : inputModule);
                    if (configuration.useChecksumModule()) {
                        req.setNewStringVariable("CHECKSUM-MODULE", configuration.getChecksumModule());
                    }
                    rs = session.submitRequest(req);
                    // success!
                    break;
                } catch (XQueryException e) {
                    // probably an XQuery syntax error - do not retry
                    // we want to know what the uri and query were.
                    String urisJoined = Utilities.join(uris, "; ");
                    logger.severe("error in document, uri = " + urisJoined);
                    logger.severe("error in query: " + query);
                    throw new SyncException(urisJoined, e);
                } catch (XccException e) {
                    retries--;
                    // we want to know which document it was
                    String urisJoined = Utilities.join(uris, "; ");
                    if (retries < 1) {
                        logger.severe("retries exhausted for " + urisJoined);
                        throw new SyncException(urisJoined, e);
                    }
                    logger.warning("error reading document: will retry ("
                            + retries + "): " + urisJoined + " due to "
                            + e.getMessage());
                    Thread.yield();
                    // if the session "went bad", we'll want a new one
                    if (null != session && !session.isClosed()) {
                        session.close();
                    }
                    session = configuration.newInputSession();
                }
            }

            if (rs != null) {
                if (!rs.hasNext()) {
                    throw new SyncException("unexpected empty document: " + uris[urisIndex]);
                }

                ResultItem[] items = rs.toResultItemArray();

                int resultIndex = 0;

                while (resultIndex < items.length) {
                    logger.fine("resultIndex " + resultIndex + "/" + items.length);
                    if (null == uris[urisIndex]) {
                        logger.fine("uri at " + urisIndex + " is null");
                        break;
                    }
                    logger.fine("reading uri: " + uris[urisIndex]);
                    resultIndex = readDocument(document, items, urisIndex, resultIndex);
                    urisIndex++;
                }
            }

        } finally {
            cleanup(session, rs);
        }
    }

    private int readDocument(DocumentInterface document, ResultItem[] items, int urisIndex, int resultIndex) throws SyncException {
        MetadataInterface metadata = document.newMetadata();

        // handle node-kind, always present
        String format = items[resultIndex].asString();
        logger.finer("format = " + format);
        metadata.setFormat(format);
        resultIndex++;

        // handle collections, may not be present
        while (resultIndex < items.length && items[resultIndex].getItemType() == ValueType.XS_STRING) {
            if (!copyCollections) {
                resultIndex++;
                continue;
            }
            metadata.addCollection(items[resultIndex].asString());
            resultIndex++;
        }

        // handle permissions, may not be present
        while (resultIndex < items.length && ValueType.ELEMENT == items[resultIndex].getItemType()) {
            if (!copyPermissions) {
                resultIndex++;
                continue;
            }
            readPermission((XdmElement) items[resultIndex].getItem(), metadata);
            resultIndex++;
        }

        // handle quality, always present even if not requested (barrier)
        metadata.setQuality((XSInteger) items[resultIndex].getItem());
        resultIndex++;

        // handle document-node, always present
        if (metadata.isBinary()) {
            document.setContent(urisIndex, items[resultIndex].asInputStream());
        } else {
            document.setContent(urisIndex, items[resultIndex].asReader());
        }
        resultIndex++;

        // handle prop:properties node, optional
        // if not present, there will be a 0 as a marker
        if (copyProperties && ValueType.ELEMENT == items[resultIndex].getItemType()) {
            String pString = items[resultIndex].asString();
            if (pString != null) {
                metadata.setProperties(pString);
            }
            resultIndex++;
        }

        // handle hash value, optional
        if (configuration.useChecksumModule()) {
            String hashValue = items[resultIndex].asString();
            metadata.setHashValue(hashValue);
            logger.fine("hashValue = " + hashValue);
            resultIndex++;
        }

        // verify end-of-record marker, which should be 0
	// this is a must.  If this verification fails, we have a parsing problem
        if (ValueType.XS_INTEGER != items[resultIndex].getItemType()) {
            throw new SyncException("unexpected "
                    + items[resultIndex].getItemType() + " "
                    + items[resultIndex].asString() + ", expected "
                    + ValueType.XS_INTEGER + " 0");
        }
        resultIndex++;

        document.setMetadata(urisIndex, metadata);
        return resultIndex;
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
     * @param permissionElement
     * @param metadata
     * @throws SyncException
     */
    private void readPermission(XdmElement permissionElement, MetadataInterface metadata) throws SyncException {
        // permission: turn into a ContentPermission object
        // each permission is a sec:permission element.
        // children:
        // sec:capability ("read", "insert", "update")
        // and sec:role xs:unsignedLong (but we need string)
        logger.fine("permissionElement = " + permissionElement.asString());
        try {
            Element permissionW3cElement = permissionElement.asW3cElement();
            logger.fine("permissionElement = " + permissionW3cElement.toString());

            NodeList capabilities = permissionW3cElement.getElementsByTagName("sec:capability");
            NodeList roles = permissionW3cElement.getElementsByTagName("sec:role-name");
            Node role;
            Node capability;
            if (0 < roles.getLength() && 0 < capabilities.getLength()) {
                role = roles.item(0);
                capability = capabilities.item(0);
                metadata.addPermission(capability.getTextContent(), role.getTextContent());
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
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new SyncException(e);
        }
    }

    /**
     * @throws RequestException
     * 
     */
    private synchronized void initQuery()  {
        if (null != query) {
            return;
        }

        if (null != inputModule) {
            logger.info("using " + Configuration.INPUT_MODULE_URI_KEY + "=" + inputModule);
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
        StringBuilder localQuery = new StringBuilder(Session.XQUERY_VERSION_1_0_ML);

        if (!isIndented) {
            localQuery.append("declare boundary-space preserve;\n");
            localQuery.append("declare option xdmp:output \"indent=no\";\n");
        }

        if (configuration.useChecksumModule())
            localQuery.append("declare variable $CHECKSUM-MODULE as xs:string external;\n");

        // prolog - some variables are per-input
        localQuery.append("declare variable $MODULE-URI as xs:string external;\n");

        // we should not normally have to guard against multiple docs per URI
        String predicate = configuration
                .isRepairMultipleDocumentsPerUri() ? "[1]" : "";

        for (int i = 0; i < size; i++) {
            // TODO - support for naked properties?
            localQuery.append("declare variable $URI-").append(i)
                .append(" as xs:string external;\n")
                .append("declare variable $DOC-").append(i)
                .append(" as document-node() := \n")
                .append("  if ($URI-").append(i).append(" eq '') then document { () }\n")
                .append("  else if ($MODULE-URI) then xdmp:invoke(\n")
                .append("    $MODULE-URI, (xs:QName('URI'), $URI-").append(i).append("))").append(predicate).append("\n")
                .append("  else doc($URI-").append(i).append(")").append(predicate).append("\n").append(";\n"
                // a document may contain multiple root nodes
                // we will prefer the element(), if present
            ).append("declare variable $ROOT-").append(i).append(" as node()? := \n"
                // no need to check for document-node, attribute, namespace
            ).append(" (\n").append("  $DOC-").append(i)
                .append("/element(), $DOC-").append(i)
                .append("/binary(), $DOC-").append(i)
                .append("/comment(),\n").append("  $DOC-").append(i)
                .append("/processing-instruction(), $DOC-")
                .append(i).append("/text()\n").append(" )[1] ;\n");
        }

        // body, once per input
        for (int i = 0; i < size; i++) {
            localQuery.append(0 == i ? "\n" : ",\n");

            // NB - empty document is equivalent to an empty text node
            localQuery.append("if ($ROOT-").append(i).append(") then xdmp:node-kind($ROOT-").append(i).append(") else 'text',\n");

            if (copyCollections) {
                localQuery.append("if ($URI-").append(i).append(" eq '') then ()\n").append("else xdmp:document-get-collections($URI-").append(i).append("),\n");
            }

            // use node for permissions, since we walk the tree
            if (copyPermissions) {
                localQuery.append("let $list := \n" + "  if ($URI-").append(i).append(" eq '') then ()\n")
                    .append("else xdmp:document-get-permissions($URI-").append(i).append(")\n")
                    .append("let $query := concat(\n").append("' import module ''http://marklogic.com/xdmp/security''").append(" at ''/MarkLogic/security.xqy'';',\n")
                    .append("' declare variable $LIST as element(sec:permissions) external;',\n")
                    .append("' for $p in $LIST/sec:permission',\n").append("' return element sec:permission {',\n").
                    append("'  $p/@*, $p/node(), sec:get-role-names($p/sec:role-id)',\n")
                    .append("' }'\n").append(")\n").append("where exists($list)\n" // x
                ).append("return xdmp:eval(\n" // x
                ).append("  $query,\n" // x
                ).append("  (xs:QName('LIST'),\n").append("   element sec:permissions { $list }),\n")
                    .append("  <options xmlns=\"xdmp:eval\">\n")
                    .append("    <database>{ xdmp:security-database() }</database>\n").append("  </options> ),\n");
            }

            // quality acts as a marker between permissions and the node
            if (copyQuality) {
                localQuery.append("if ($URI-").append(i).append(" eq '') then 0\n").append("else xdmp:document-get-quality($URI-").append(i).append("),\n");
            } else {
                localQuery.append("0,");
            }

            localQuery.append("$DOC-").append(i).append(",\n");

            if (copyProperties) {
                localQuery.append("if ($URI-").append(i).append(" eq '') then ()\n")
                    .append("else xdmp:document-properties($URI-").append(i).append(")/prop:properties,\n");
            } else {
                localQuery.append("(),\n");
            }

            if (configuration.useChecksumModule()) {
                localQuery.append("if ($URI-").append(i).append(" eq '') then ()\n")
                    .append("else xdmp:invoke($CHECKSUM-MODULE, (xs:QName('URI'), $URI-").append(i).append(")),\n");
            } else {
                localQuery.append("(),\n");
            }
            
            // end-of-record marker
            localQuery.append("0\n");
        }

        query = localQuery.toString();
    }
}
