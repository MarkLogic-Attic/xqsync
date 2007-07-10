/**
 * Copyright (c) 2007 Mark Logic Corporation. All rights reserved.
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
package com.marklogic.ps.tests;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.TestCase;

import org.xml.sax.SAXException;

import com.marklogic.ps.Connection;
import com.marklogic.ps.Session;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.xqsync.XQSyncDocument;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class DocumentTest extends TestCase {

    public void testEscaping() throws UnsupportedEncodingException {
        SimpleLogger logger = SimpleLogger.getSimpleLogger();

        String testString = "http://foo.com/bar baz/";
        // URI uri = new URI(null, null, testString, null, null);
        // logger.info(uri.toString());
        // logger.info(uri.toASCIIString());

        String expected = testString;
        XQSyncDocument.setLogger(logger);
        XQSyncDocument doc = new XQSyncDocument(testString, true);
        testString = doc.getOutputUri();
        // logger.info(testString);
        assertEquals(testString, expected);
        testString = doc.composeOutputUri(true);
        // logger.info(testString);
        assertEquals(testString, expected);
    }

    public void testPermissions() throws URISyntaxException,
            XccException, IOException, ParserConfigurationException,
            SAXException {
        SimpleLogger logger = SimpleLogger.getSimpleLogger();
        Properties props = new Properties();
        props.setProperty(SimpleLogger.LOG_LEVEL, "INFO");
        props.setProperty(SimpleLogger.LOG_HANDLER, "CONSOLE");
        logger.configureLogger(props);
        URI uri = new URI("xcc://admin:admin@localhost:9000/");
        Connection c = new Connection(uri);
        Session sess = (Session) c.newSession();
        String documentUri = this.getClass().getName()
                + "/testPermissions.xml";

        String documentString = "<!-- this is a test -->\n"
                + "<test id=\"foo\"/>\n";
        ContentCreateOptions createOptions = ContentCreateOptions
                .newXmlInstance();

        // write the test document
        Content content = ContentFactory.newContent(documentUri,
                documentString, createOptions);
        sess.insertContent(content);

        // set the permissions
        AdhocQuery req = sess
                .newAdhocQuery("define variable $URI as xs:string external\n"
                        + "let $perms := xdmp:permission('admin', 'read')\n"
                        + "return xdmp:document-set-permissions($URI, $perms)\n");
        req.setNewStringVariable("URI", documentUri);
        sess.submitRequest(req);

        // retrieve the test document
        XQSyncDocument doc = new XQSyncDocument(sess, documentUri, true,
                true, false);
        String retrievedXml = new String(doc.getContentBytes());

        // test the round-trip of the XML
        assertEquals(documentString, retrievedXml);

        // test the permissions
        ContentPermission[] permissions = doc.getMetadata()
                .getPermissions();
        logger.fine("found permissions " + permissions.length);
        for (int i = 0; i < permissions.length; i++) {
            logger.finer("permission[" + i + "] = " + permissions[i]);
        }
        assertEquals(1, permissions.length);
        assertEquals(permissions[0].getCapability(),
                ContentCapability.READ);
        assertEquals(permissions[0].getRole(), "admin");

        // on success, delete the test document
        // (otherwise we might inspect it)
        req = sess
                .newAdhocQuery("define variable $URI as xs:string external\n"
                        + "xdmp:document-delete($URI)\n");
        req.setNewStringVariable("URI", documentUri);
        sess.submitRequest(req);

        sess.close();
    }
}
