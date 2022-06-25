/**
 * Copyright (c) 2007-2022 MarkLogic Corporation. All rights reserved.
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

import java.util.Properties;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.xqsync.Configuration;
import com.marklogic.ps.xqsync.FilePathReader;
import com.marklogic.ps.xqsync.FilePathWriter;
import com.marklogic.ps.xqsync.XQSyncDocument;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class XQSyncDocumentTest {

    @Test
    public void testEscaping() throws Exception {
        String testString = "http://foo.com/bar baz/";
        String expected = testString;
        Configuration config = new Configuration();
        Properties properties = new Properties();
        properties.setProperty(Configuration.INPUT_PATH_KEY, "/dev/null");
        properties.setProperty(Configuration.OUTPUT_PATH_KEY, "/dev/null");
        config.setLogger(SimpleLogger.getSimpleLogger());
        config.setProperties(properties);
        config.configure();
        FilePathReader reader = new FilePathReader(config);
        FilePathWriter writer = new FilePathWriter(config);
        XQSyncDocument doc = new XQSyncDocument(new String[] { testString }, reader, writer, config);
        testString = doc.getOutputUri(0);
        assertEquals(expected, testString);
        // testString = doc.getOutputUri(true);
        // assertEquals(testString, expected);
    }

}
