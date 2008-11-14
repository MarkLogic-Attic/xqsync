/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
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
    public void read(String _uri, DocumentInterface _document)
            throws SyncException {
        if (null == _uri) {
            throw new SyncException("null uri");
        }
        if (null == _document) {
            throw new SyncException("null document");
        }

        ResultSequence rs = null;
        Session session = null;
        try {
            // in case the server is unreliable, we try three times
            int retries = 3;
            while (retries > 0) {
                try {
                    // retry around all session-related objects
                    session = configuration.newInputSession();
                    RequestOptions opts = session
                            .getDefaultRequestOptions();
                    if (null != timestamp) {
                        opts.setEffectivePointInTime(timestamp);
                    }
                    opts.setResultBufferSize(configuration
                            .inputResultBufferSize());
                    Request req = session.newAdhocQuery(query, opts);
                    req.setNewStringVariable("URI", _uri);
                    req.setNewStringVariable("MODULE-URI",
                            (null == inputModule) ? "" : inputModule);

                    rs = session.submitRequest(req);
                    // success!
                    break;
                } catch (XQueryException e) {
                    // probably an XQuery syntax error - do not retry
                    // we want to know what the uri and query were.
                    logger.severe("error in document, uri = " + _uri);
                    logger.severe("error in query: " + query);
                    throw new SyncException(_uri, e);
                } catch (XccException e) {
                    retries--;
                    // we want to know which document it was
                    if (retries < 1) {
                        logger.severe("retries exhausted for " + _uri);
                        throw new SyncException(_uri, e);
                    }
                    logger.warning("error reading document: will retry ("
                            + retries + "): " + _uri + " due to "
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
                        + _uri);
            }

            MetadataInterface metadata = _document.newMetadata();
            ResultItem[] items = rs.toResultItemArray();

            // handle node-kind, always present
            String format = items[0].asString();
            logger.finer("format = " + format);
            metadata.setFormat(format);

            int index = 1;

            // handle collections, may not be present
            while (index < items.length
                    && items[index].getItemType() == ValueType.XS_STRING) {
                if (!copyCollections) {
                    index++;
                    continue;
                }
                metadata.addCollection(items[index].asString());
                index++;
            }

            // handle permissions, may not be present
            while (index < items.length
                    && ValueType.ELEMENT == items[index].getItemType()) {
                if (!copyPermissions) {
                    index++;
                    continue;
                }
                readPermission((XdmElement) items[index].getItem(),
                        metadata);
                index++;
            }

            // handle quality, always present even if not requested (barrier)
            metadata.setQuality((XSInteger) items[index].getItem());
            index++;

            // handle document-node, always present
            if (metadata.isBinary()) {
                _document.setContent(items[index].asInputStream());
            } else {
                _document.setContent(items[index].asReader());
            }
            index++;

            // handle prop:properties node, optional
            if (copyProperties && index < items.length) {
                String pString = items[index].asString();
                if (pString != null) {
                    metadata.setProperties(pString);
                }
            }

            _document.setMetadata(metadata);

            // pre-empt the finally block
            cleanup(session, rs);

        } finally {
            cleanup(session, rs);
        }
    }

    private void cleanup(Session session, ResultSequence rs) {
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
        // first is node-kind
        // then collection strings (if requested and present)
        // then permission nodes (if requested and present)
        // then quality integer, always present (default 0)
        // then the document-node
        // then property node (if requested and present)
        // one wrinkle: get-permissions() returns sec:permission/sec:role-id,
        // but our callers need the role-name! this gets ugly...
        // must wrap the list of permissions so we can pass it...
        //
        // normally I'd put this code in a module,
        // but I want this program to be self-contained
        query = Session.XQUERY_VERSION_0_9_ML
                + "define variable $URI as xs:string external\n"
                + "define variable $MODULE-URI as xs:string external\n"
                + "define variable $DOC as document-node() {\n"
                + "  if ($MODULE-URI) then xdmp:invoke(\n"
                + "    $MODULE-URI, (xs:QName('URI'), $URI))\n"
                + "  else doc($URI)\n"
                + "}\n"
                // a document may contain multiple root nodes
                // we will prefer the element(), if present
                + "define variable $ROOT as node()? {\n"
                // no need to check for document-node, attribute, namespace
                + " (\n"
                + "  $DOC/element(), $DOC/binary(), $DOC/comment(),\n"
                + "  $DOC/processing-instruction(), $DOC/text()\n"
                + " )[1] }\n"
                // NB - empty document is equivalent to an empty text node
                + "if ($ROOT) then node-kind($ROOT) else 'text',\n";

        if (copyCollections) {
            query += "xdmp:document-get-collections($URI),\n";
        }

        // use node for permissions, since we walk the tree
        if (copyPermissions) {
            query += "let $list := xdmp:document-get-permissions($URI)\n"
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

        // quality acts as a barrier between permissions and the node
        if (copyQuality) {
            query += "xdmp:document-get-quality($URI),\n";
        } else {
            query += "0,";
        }

        query += "$DOC,\n";

        if (copyProperties) {
            query += "xdmp:document-properties($URI)\n";
        } else {
            query += "()\n";
        }
    }

}
