/**
 * Copyright (c) 2017-2022 MarkLogic Corporation. All rights reserved.
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

import com.marklogic.ps.SimpleLogger;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

/**
 *
 */
public class XQSyncTest {
/*
    @Test
    public void main() throws Exception {
        String[] args = new String[]{};
        XQSync.main(args);
    }
*/
    @Test (expected = NullPointerException.class)
    public void mainWithNullArgs() throws Exception {
        XQSync.main(null);
    }

    @Test
    public void initConfiguration() throws Exception {
        String expected = Configuration.CONFIGURATION_CLASSNAME_DEFAULT;
        Properties properties = new Properties();
        properties.setProperty(Configuration.CONFIGURATION_CLASSNAME_KEY, expected);
        properties.setProperty(Configuration.INPUT_CONNECTION_STRING_KEY, "xcc://xqsync-test-user:xqsync-test-password@localhost:9000");
        properties.setProperty(Configuration.OUTPUT_CONNECTION_STRING_KEY, "xcc://xqsync-test-user:xqsync-test-password@localhost:9000");
        Configuration configuration = XQSync.initConfiguration(SimpleLogger.getSimpleLogger(), properties);
        assertNotNull(configuration);
        assertEquals(expected, configuration.getConfigurationClassName());
    }

    @Test (expected = FatalException.class)
    public void initConfigurationWithNullLoggerAndConfigurationClassname() throws Exception {
        String expected = "foo";
        Properties properties = new Properties();
        properties.setProperty(Configuration.CONFIGURATION_CLASSNAME_KEY, expected);
        Configuration configuration = XQSync.initConfiguration(null, properties);
        //should this throw NPE or just not attempt to log if the logger is null?
    }

    @Test (expected = FatalException.class)
    public void initConfigurationWithoutConfigurationClassname() throws Exception {
        Properties properties = new Properties();
        XQSync.initConfiguration(null, properties);
    }

    @Test (expected = FatalException.class)
    public void initConfigurationWithNullConfigAndProperties() throws Exception {
        XQSync.initConfiguration(null, null);
    }

    @Test
    public void getClassLoader() {
        assertNotNull(XQSync.getClassLoader());
    }

}