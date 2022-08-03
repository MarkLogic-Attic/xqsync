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
import org.junit.Test;

import static com.marklogic.ps.xqsync.Configuration.INPUT_COLLECTION_URI_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class ConfigurationTest {
    @Test
    public void testConfiguration() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(Configuration.CONFIGURATION_CLASSNAME_KEY, ExampleConfiguration.class.getCanonicalName());
        properties.setProperty(Configuration.INPUT_CONNECTION_STRING_KEY, "test");
        Configuration config = XQSync.initConfiguration(SimpleLogger.getSimpleLogger(), properties);
        config.close();
        assertEquals(config.getClass().getCanonicalName(), ExampleConfiguration.class.getCanonicalName());
    }

    @Test
    public void isValidConnectionString() {
        String uriSuffix = "xqsync-test-user:xqsync-test-password@localhost:9000";
        Configuration configuration = new Configuration();
        assertTrue(configuration.isValidConnectionString("xcc://" + uriSuffix));
        assertTrue(configuration.isValidConnectionString("xccs://" + uriSuffix));
        assertTrue(configuration.isValidConnectionString("xdbc://" + uriSuffix));
        assertFalse(configuration.isValidConnectionString("http://" + uriSuffix));
    }

    @Test
    public void testGetInputCollectionUris() {
        Properties properties = new Properties();
        properties.setProperty(INPUT_COLLECTION_URI_KEY, "foo bar");
        Configuration configuration = new Configuration();
        configuration.properties = properties;
        String[] inputCollectionUris = configuration.getInputCollectionUris();
        assertEquals(2, inputCollectionUris.length);
        assertEquals("foo", inputCollectionUris[0]);
    }

    @Test
    public void testGetInputCollectionUrisWhenNull() {
        Properties properties = new Properties();
        Configuration configuration = new Configuration();
        configuration.properties = properties;
        assertNull(configuration.getInputCollectionUris());
    }
}
